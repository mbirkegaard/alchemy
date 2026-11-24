/**
 * The replicas suite's front: a public Railway Service in the cluster's
 * environment. It calls the replica entity through the pool's gateway, and
 * asks one runner, at its own private address, to crash.
 */
import * as Railway from "@alchemy.run/railway";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { Partition, Site } from "../../../Railway/fixtures/suite-env.ts";
import { CRASH_PORT } from "./replica-live.ts";
import { Replicas } from "./replica.ts";

export default class ReplicasFront extends Railway.Service<ReplicasFront>()(
  "ClusterReplicasFront",
  {
    project: Site,
    environment: Partition,
    main: import.meta.url,
    port: 3000,
    healthcheck: "/ready",
  },
  Effect.gen(function* () {
    const replicas = yield* Replicas;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const [route, id = "x", tx, amount] = new URL(request.url, "http://front").pathname
          .split("/")
          .filter((part) => part.length > 0);
        switch (route) {
          case "ready":
            return HttpServerResponse.text("ok");
          case "where":
            return yield* HttpServerResponse.json(yield* (yield* replicas.getByName(id)).Where());
          case "credit":
            return yield* HttpServerResponse.json({
              value: yield* (yield* replicas.getByName(id)).Credit({
                txId: tx!,
                amount: Number(amount),
              }),
            });
          case "crash": {
            // `id` is a replica's own address, as `where` reported it.
            const response = yield* HttpClient.post(`http://[${id}]:${CRASH_PORT}/crash`).pipe(
              Effect.provide(FetchHttpClient.layer),
            );
            return HttpServerResponse.text(String(response.status));
          }
          default:
            return HttpServerResponse.empty({ status: 404 });
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.succeed(HttpServerResponse.text(String(cause), { status: 500 })),
        ),
      ),
    };
  }),
) {}
