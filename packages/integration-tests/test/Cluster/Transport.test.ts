import { spawn } from "node:child_process";
import { createServer as createHttpServer } from "node:http";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
/**
 * How calls reach a pool when the process at the other end restarts or goes
 * away, on real local processes and sockets: a one-runner pool and its
 * gateway run as a child process, reached directly or through a relay that
 * the test cuts.
 */
import { makeNamespace } from "@alchemy.run/effect-cluster/internal/Gateway";
import { describe, expect, it } from "@alchemy.run/test";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import { freePort, relayTo } from "./fixtures/relay.ts";
import { AnySlow, TOKEN } from "./fixtures/transport/slow.ts";

const poolProgram = fileURLToPath(new URL("./fixtures/transport/pool.ts", import.meta.url));

/** Run a pool serving its gateway on `gatewayPort`, killed with the test's scope. */
const runPool = (gatewayPort: number) =>
  Effect.gen(function* () {
    const runnerPort = yield* freePort;
    yield* Effect.acquireRelease(
      Effect.sync(() =>
        spawn(process.execPath, [poolProgram, String(gatewayPort), String(runnerPort)], {
          stdio: "ignore",
        }),
      ),
      (child) => Effect.sync(() => child.kill("SIGKILL")),
    );
  });

interface SlowClient {
  readonly Call: (payload: { id: string; ms: number }) => Effect.Effect<number, unknown>;
  readonly Runs: (payload: { id: string }) => Effect.Effect<number, unknown>;
}

/** A caller outside the cluster, reaching the gateway at `port`. */
const slowAt = (port: number, id: string) =>
  makeNamespace(AnySlow, { url: `http://127.0.0.1:${port}`, token: TOKEN })
    .getByName(id)
    .pipe(Effect.map((client) => client as unknown as SlowClient));

const call = (port: number, id: string, ms: number) =>
  Effect.flatMap(slowAt(port, id), (slow) => slow.Call({ id, ms }));

/** How many times `id` has run, asked of another instance: `id`'s own is busy running it. */
const runsOf = (port: number, id: string) =>
  Effect.flatMap(slowAt(port, `count-${id}`), (slow) => slow.Runs({ id }));

/** Start a pool and wait until it answers calls. */
const started = Effect.gen(function* () {
  const gatewayPort = yield* freePort;
  yield* runPool(gatewayPort);
  yield* runsOf(gatewayPort, "warm").pipe(
    Effect.retry({ schedule: Schedule.spaced("100 millis"), times: 100 }),
  );
  return gatewayPort;
});

describe("Cluster calls through a gateway", { tags: ["provider:cluster", "unit", "local"] }, () => {
  it.live(
    "a call sent before its gateway listens is sent again once it does, and runs once",
    () =>
      Effect.gen(function* () {
        const gatewayPort = yield* freePort;
        // Nothing listens yet: the call's connection is refused, as while a
        // runner restarts. The pool starts half a second later.
        yield* runPool(gatewayPort).pipe(Effect.delay("500 millis"), Effect.forkScoped);
        expect(yield* call(gatewayPort, "before-listen", 0)).toEqual(1);
        expect(yield* runsOf(gatewayPort, "before-listen")).toEqual(1);
      }),
    { timeout: 30_000 },
  );

  it.live(
    "a call that reached its gateway is never sent again, even when its connection is cut",
    () =>
      Effect.gen(function* () {
        const gatewayPort = yield* started;
        const relay = yield* relayTo(gatewayPort);
        const sent = yield* call(relay.port, "cut", 2_000).pipe(Effect.exit, Effect.forkScoped);
        // The handler is running: the call reached the gateway.
        yield* runsOf(gatewayPort, "cut").pipe(
          Effect.repeat({ until: (runs) => runs === 1, schedule: Schedule.spaced("20 millis") }),
        );
        expect(yield* relay.cut).toBeGreaterThan(0);
        expect(Exit.isFailure(yield* Fiber.join(sent))).toBe(true);
        // A caller that sent it again would have run it again by now.
        yield* Effect.sleep("3 seconds");
        expect(yield* runsOf(gatewayPort, "cut")).toEqual(1);
      }),
    { timeout: 30_000 },
  );

  it.live(
    "a slow call to a gateway that is up is waited for, and runs once",
    () =>
      Effect.gen(function* () {
        const gatewayPort = yield* started;
        expect(yield* call(gatewayPort, "slow", 4_000)).toEqual(1);
        expect(yield* runsOf(gatewayPort, "slow")).toEqual(1);
      }),
    { timeout: 30_000 },
  );
});

const requestProgram = fileURLToPath(
  new URL("./fixtures/transport/runner-request.ts", import.meta.url),
);

/**
 * A runner's peer: answers `/hit?id=&ms=` after `ms`, counting the requests
 * for each `id`.
 */
const peer = Effect.acquireRelease(
  Effect.callback<{ port: number; hits: Map<string, number>; close: () => void }>((resume) => {
    const hits = new Map<string, number>();
    const server = createHttpServer((request, response) => {
      const url = new URL(request.url ?? "/", "http://peer");
      const id = url.searchParams.get("id") ?? "";
      hits.set(id, (hits.get(id) ?? 0) + 1);
      setTimeout(() => response.end("ok"), Number(url.searchParams.get("ms") ?? 0));
    });
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      resume(
        Effect.succeed({
          port: typeof address === "object" && address !== null ? address.port : 0,
          hits,
          close: () => server.close(),
        }),
      );
    });
  }),
  ({ close }) => Effect.sync(close),
);

interface Answer {
  readonly url: string;
  readonly ok: boolean;
  readonly ms: number;
  readonly error?: string;
}

/**
 * A Node process sending requests as a runner does (runners run on Node);
 * `get` waits for its answer.
 */
const runnerSending = Effect.gen(function* () {
  const answers = yield* Queue.unbounded<Answer>();
  const child = yield* Effect.acquireRelease(
    Effect.sync(() => spawn("node", [requestProgram], { stdio: ["pipe", "pipe", "ignore"] })),
    (child) => Effect.sync(() => child.kill("SIGKILL")),
  );
  createInterface({ input: child.stdout! }).on("line", (line) =>
    Queue.offerUnsafe(answers, JSON.parse(line) as Answer),
  );
  return {
    get: (url: string) =>
      Effect.sync(() => child.stdin!.write(`${url}\n`)).pipe(Effect.andThen(Queue.take(answers))),
  };
});

describe(
  "Cluster requests between runners",
  { tags: ["provider:cluster", "unit", "local"] },
  () => {
    it.live(
      "a request to a runner that vanished without closing its connection fails within seconds",
      () =>
        Effect.gen(function* () {
          const server = yield* peer;
          const relay = yield* relayTo(server.port);
          const runner = yield* runnerSending;
          // A connection to the peer is open and kept for the next request.
          expect((yield* runner.get(`http://127.0.0.1:${relay.port}/hit?id=warm`)).ok).toBe(true);
          // The peer vanishes as a removed Railway replica does: the open
          // connection takes the next request and answers nothing.
          yield* relay.goSilent;
          const answer = yield* runner
            .get(`http://127.0.0.1:${relay.port}/hit?id=gone`)
            .pipe(Effect.timeout("10 seconds"));
          expect(relay.swallowed()).toBeGreaterThan(0);
          // Failed, so Effect Cluster routes it again, instead of waiting.
          expect(answer.ok).toBe(false);
          expect(answer.ms).toBeLessThan(5_000);
        }),
      { timeout: 30_000 },
    );

    it.live(
      "a slow answer from a runner that is up is waited for, and sent once",
      () =>
        Effect.gen(function* () {
          const server = yield* peer;
          const relay = yield* relayTo(server.port);
          const runner = yield* runnerSending;
          const answer = yield* runner.get(`http://127.0.0.1:${relay.port}/hit?id=slow&ms=4000`);
          expect(answer.ok).toBe(true);
          expect(answer.ms).toBeGreaterThanOrEqual(4_000);
          expect(server.hits.get("slow")).toEqual(1);
        }),
      { timeout: 30_000 },
    );
  },
);

const gatewayStopProgram = fileURLToPath(
  new URL("./fixtures/transport/gateway-stop.ts", import.meta.url),
);

describe("Cluster gateway shutdown", { tags: ["provider:cluster", "unit", "local"] }, () => {
  it.live(
    "a stopping gateway lets a request left open run for 2 seconds, not 20",
    () =>
      Effect.gen(function* () {
        const port = yield* freePort;
        const lines = yield* Queue.unbounded<string>();
        // On Node, as runners run: Bun's server closes open connections itself.
        const child = yield* Effect.acquireRelease(
          Effect.sync(() =>
            spawn("node", [gatewayStopProgram, String(port)], {
              stdio: ["pipe", "pipe", "ignore"],
            }),
          ),
          (child) => Effect.sync(() => child.kill("SIGKILL")),
        );
        createInterface({ input: child.stdout! }).on("line", (line) =>
          Queue.offerUnsafe(lines, line),
        );
        expect(yield* Queue.take(lines)).toEqual("ready");
        const response = yield* Effect.promise(() => fetch(`http://127.0.0.1:${port}/`));
        expect(response.status).toEqual(200);
        child.stdin!.write("stop\n");
        const stoppedMs = Number(yield* Queue.take(lines).pipe(Effect.timeout("30 seconds")));
        expect(stoppedMs).toBeLessThan(5_000);
      }),
    { timeout: 45_000 },
  );
});
