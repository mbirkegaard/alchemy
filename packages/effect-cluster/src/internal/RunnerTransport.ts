/**
 * Effect Cluster's HTTP transport between runners, as `NodeClusterHttp.layer`
 * builds it, with one change: a request one runner sends another that has no
 * response after a second checks that the runner it went to is still there,
 * and fails once it is not, so Effect Cluster routes the message again.
 *
 * A runner the platform removes (a Railway replica replaced in a deploy) can
 * vanish without closing its connections. Its address stops answering, and a
 * request sent on a connection still open to it waits for minutes: Effect
 * Cluster routes a message again only once its send fails, so the caller's own
 * timeout was the only bound. That happens whenever a runner forwards a
 * message on a shard view a moment older than the removal. A runner that is
 * up accepts a new TCP connection at once, even while its handlers are busy,
 * so a request fails only when its runner is gone, never because a handler is
 * slow (sending it again then could run it twice).
 */
import { connect } from "node:net";
import { NodeClusterHttp, NodeHttpClient } from "@effect/platform-node";
import * as Cache from "effect/Cache";
import { HttpRunner, RunnerHealth, Runners, ShardingConfig } from "effect/cluster";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import * as Layer from "effect/Layer";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as Schedule from "effect/Schedule";

/** How long a request waits for its response before checking its runner. */
const UNANSWERED = Duration.seconds(1);
/** How often a request still waiting checks again; also how long a check is reused. */
const RECHECK = Duration.seconds(2);
/** How long a new connection may take: a lost SYN is sent again after 1 s. */
const CONNECT_MS = 2_000;

/** Does `host:port` accept a new TCP connection in time? */
export const accepts = (host: string, port: number) =>
  Effect.callback<boolean>((resume) => {
    let settled = false;
    const socket = connect({ host, port, timeout: CONNECT_MS });
    const settle = (accepted: boolean) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resume(Effect.succeed(accepted));
    };
    socket.once("connect", () => settle(true));
    socket.once("timeout", () => settle(false));
    socket.once("error", () => settle(false));
    return Effect.sync(() => socket.destroy());
  });

/**
 * An `HttpClient` for requests between runners that fails one whose runner
 * no longer accepts connections. A runner's check is shared by every request
 * to it for {@link RECHECK}.
 */
export const runnerHttpClient = Layer.effect(
  HttpClient.HttpClient,
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const reachable = yield* Cache.make({
      capacity: 1024,
      timeToLive: RECHECK,
      lookup: (address: string) => {
        const { hostname, port } = new URL(address);
        return accepts(hostname.replace(/^\[|\]$/g, ""), Number(port));
      },
    });
    return HttpClient.transform(client, (response, request) => {
      const { origin } = new URL(request.url);
      const gone = Effect.sleep(UNANSWERED).pipe(
        Effect.andThen(Cache.get(reachable, origin)),
        Effect.repeat({ while: (up) => up, schedule: Schedule.spaced(RECHECK) }),
        Effect.andThen(
          Effect.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                request,
                description: `Effect Cluster runner ${origin} no longer accepts connections`,
              }),
            }),
          ),
        ),
      );
      return Effect.raceFirst(response, gone);
    });
  }),
).pipe(Layer.provide(NodeHttpClient.layerUndici));

/**
 * The runner's Effect Cluster over HTTP: `NodeClusterHttp.layer` with
 * `transport: "http"` and `storage: "byo"`, sending to other runners through
 * {@link runnerHttpClient}.
 */
export const layer = (shardingConfig: Partial<ShardingConfig.ShardingConfig["Service"]>) => {
  const clientProtocol = HttpRunner.layerClientProtocolHttpDefault.pipe(
    Layer.provide(runnerHttpClient),
  );
  return HttpRunner.layerHttp.pipe(
    Layer.provide([NodeClusterHttp.layerHttpServer, runnerHttpClient]),
    Layer.provide(
      RunnerHealth.layerPing.pipe(Layer.provide(Runners.layerRpc), Layer.provide(clientProtocol)),
    ),
    Layer.provide(ShardingConfig.layerFromEnv(shardingConfig)),
    Layer.provide(RpcSerialization.layerSchemaBinary({})),
  );
};

/**
 * A cluster client with no runner of its own (an external program's
 * gateway): `NodeClusterHttp.layer` with `clientOnly` and `storage: "byo"`,
 * sending to runners through {@link runnerHttpClient}.
 */
export const layerClientOnly = (
  shardingConfig: Partial<ShardingConfig.ShardingConfig["Service"]>,
) =>
  HttpRunner.layerHttpClientOnly.pipe(
    Layer.provide(runnerHttpClient),
    Layer.provide(ShardingConfig.layerFromEnv(shardingConfig)),
    Layer.provide(RpcSerialization.layerSchemaBinary({})),
  );
