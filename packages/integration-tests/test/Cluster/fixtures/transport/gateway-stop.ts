import { createInterface } from "node:readline";
/**
 * A gateway's HTTP server whose one response never ends, as its own (Node)
 * process: `gateway-stop.ts <port>`. Prints `ready` once listening; on
 * `stop` from stdin, stops the server and prints how long that took, in ms.
 */
import { gatewayHttpServer } from "@alchemy.run/effect-cluster/internal/GatewayServer";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

const port = Number(process.argv[2]);
// A response that never ends, as a live read left open through the gateway.
const endless = Stream.concat(Stream.make(new TextEncoder().encode("open\n")), Stream.never);

Effect.runFork(
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    yield* Layer.build(
      HttpServer.serve()(Effect.succeed(HttpServerResponse.stream(endless))).pipe(
        Layer.provide(gatewayHttpServer(port)),
      ),
    ).pipe(Scope.provide(scope));
    console.log("ready");
    yield* Effect.callback<void>((resume) => {
      createInterface({ input: process.stdin }).once("line", () => resume(Effect.void));
    });
    const started = Date.now();
    yield* Scope.close(scope, Exit.void);
    console.log(String(Date.now() - started));
    process.exit(0);
  }),
);
