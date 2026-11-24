import * as Cluster from "@alchemy.run/effect-cluster";
import * as Railway from "@alchemy.run/railway";
import { describe, expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Core from "@alchemy.run/test/Core";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import { railwayApi } from "../Railway/limit.ts";
import { conformanceTests, NotReady, waitForReady } from "./conformance.ts";
import ReplicasFront from "./fixtures/railway-replicas/front.ts";
import OneLive from "./fixtures/railway-replicas/one-live.ts";
import type { Replicas } from "./fixtures/railway-replicas/replica.ts";
import ThreeLive from "./fixtures/railway-replicas/three-live.ts";
import Front from "./fixtures/railway/front.ts";
import { Lives } from "./fixtures/railway/index.ts";
import { scheduledConformanceTests } from "./scheduled-conformance.ts";
import { workflowConformanceTests } from "./workflow-conformance.ts";

const testOptions = { providers: Layer.mergeAll(Railway.providers(), Cluster.providers()) };
const { test, beforeAll, afterAll } = Test.make(testOptions);
const sharedStack = Core.scratchStack(testOptions, "ClusterRailway", import.meta.url);

let baseUrl = "";

// The Effect Cluster conformance cases, against Railway Postgres and a
// runner Service per pool, called from a public Railway Service through the
// gateways on the project's private network.
describe.skipIf(!!process.env.FAST)(
  "Effect Cluster on Railway (Railway Postgres, Railway Service runners)",
  { tags: ["provider:cluster", "provider:railway", "live"] },
  () => {
    beforeAll(
      Effect.gen(function* () {
        if (!process.env.NO_DESTROY) yield* sharedStack.destroy();
        const { url } = yield* sharedStack.deploy(
          Effect.gen(function* () {
            const front = yield* Front;
            return { url: front.url };
          }).pipe(Effect.provide(Lives)),
        );
        expect(url).toBeTruthy();
        baseUrl = String(url).replace(/\/+$/, "");
        yield* waitForReady(baseUrl);
      }),
      // Only deploy and destroy drive Railway's API. The cases call the front.
      { timeout: 1_800_000, exclusive: railwayApi },
    );

    afterAll.skipIf(!!process.env.NO_DESTROY)(sharedStack.destroy(), {
      timeout: 1_200_000,
      exclusive: railwayApi,
    });

    conformanceTests(test as never, { baseUrl: () => baseUrl, kind: "live" });
    workflowConformanceTests(test as never, { baseUrl: () => baseUrl, kind: "live" });
    scheduledConformanceTests(test as never, { baseUrl: () => baseUrl, kind: "live" });
  },
);

const replicasStack = Core.scratchStack(testOptions, "ClusterRailwayReplicas", import.meta.url);
let replicasUrl = "";

/** The pool at a replica count, with its front, deployed. */
const deployReplicas = (live: Layer.Layer<Replicas, never, any>) =>
  replicasStack
    .deploy(
      Effect.gen(function* () {
        const front = yield* ReplicasFront;
        return { url: front.url };
      }).pipe(Effect.provide(live)),
    )
    .pipe(
      Effect.tap(({ url }) =>
        Effect.suspend(() => {
          replicasUrl = String(url).replace(/\/+$/, "");
          return untilAnswered<unknown>("/ready", 120);
        }),
      ),
    );

/** GET a front route and decode its JSON; a non-200 answer fails. */
const call = <T>(path: string) =>
  HttpClient.get(`${replicasUrl}${path}`).pipe(
    Effect.flatMap((response) =>
      response.status === 200
        ? (response.json as Effect.Effect<T, unknown>)
        : Effect.flatMap(response.text, (body) =>
            Effect.fail(
              new NotReady({ detail: `${path}: ${response.status} ${body.slice(0, 300)}` }),
            ),
          ),
    ),
    Effect.timeout("20 seconds"),
  );

/** Call until it answers, for at most `seconds`. */
const untilAnswered = <T>(path: string, seconds: number) =>
  call<T>(path).pipe(Effect.retry({ schedule: Schedule.spaced("1 second"), times: seconds }));

/** Where each of `n` fresh entity ids runs. */
const placements = (prefix: string, n: number) =>
  Effect.forEach(
    Array.from({ length: n }, (_, i) => `${prefix}-${i}`),
    (id) =>
      Effect.map(untilAnswered<WhereShape>(`/where/${id}`, 120), (where) => ({ id, ...where })),
    { concurrency: 6 },
  );

type WhereShape = { service: string; replica: string; addresses: string[] };

// One pool as replicas of one Railway Service: three runners in one shard
// group on one Postgres, one of them crashed, then the same pool and
// database scaled to one replica in place.
describe.skipIf(!!process.env.FAST)(
  "Effect Cluster on Railway replicas (one Service, one shard group)",
  { tags: ["provider:cluster", "provider:railway", "live"] },
  () => {
    let service = "";
    beforeAll(
      Effect.gen(function* () {
        if (!process.env.NO_DESTROY) yield* replicasStack.destroy();
        yield* deployReplicas(ThreeLive);
      }),
      { timeout: 1_800_000, exclusive: railwayApi },
    );

    afterAll.skipIf(!!process.env.NO_DESTROY)(replicasStack.destroy(), {
      timeout: 1_200_000,
      exclusive: railwayApi,
    });

    test(
      "three replicas of one Service each run some of the pool's entities",
      Effect.gen(function* () {
        const placed = yield* placements(`spread-${Date.now()}`, 30);
        expect(new Set(placed.map((p) => p.service)).size).toBe(1);
        expect(new Set(placed.map((p) => p.replica)).size).toBe(3);
        // Each replica registered under an address of its own.
        expect(new Set(placed.map((p) => p.addresses[0])).size).toBe(3);
        service = placed[0]!.service;
      }),
      { timeout: 300_000 },
    );

    test(
      "a crashed replica's entities answer again, and its acknowledged persisted messages replay once",
      Effect.gen(function* () {
        const id = `crash-${Date.now()}`;
        const victim = yield* untilAnswered<WhereShape>(`/where/${id}`, 120);
        expect((yield* call<{ value: number }>(`/credit/${id}/tx1/5`)).value).toBe(5);
        expect((yield* call<{ value: number }>(`/credit/${id}/tx2/7`)).value).toBe(12);
        const crashedAt = Date.now();
        yield* call<unknown>(`/crash/${victim.addresses[0]}`).pipe(Effect.ignore);
        // The reply to tx2 is stored: sent again, it answers 12. Run again on
        // the restarted instance's fresh balance, it would answer 7.
        const replayed = yield* untilAnswered<{ value: number }>(`/credit/${id}/tx2/7`, 180);
        expect(replayed.value).toBe(12);
        const after = yield* untilAnswered<WhereShape>(`/where/${id}`, 60);
        expect(after.service).toBe(victim.service);
        yield* Effect.log(
          `crashed ${victim.replica}; ${id} answered again after ${Date.now() - crashedAt} ms on ${after.replica}`,
        );
      }),
      { timeout: 600_000, exclusive: true },
    );

    test(
      "the pool scales to one replica in place: same Service, same database",
      Effect.gen(function* () {
        const id = `scale-${Date.now()}`;
        expect((yield* untilAnswered<{ value: number }>(`/credit/${id}/tx1/3`, 120)).value).toBe(3);
        yield* deployReplicas(OneLive);
        const placed = yield* placements(`one-${Date.now()}`, 12);
        expect(new Set(placed.map((p) => p.replica)).size).toBe(1);
        expect(placed[0]!.service).toBe(service);
        // The stored reply outlived the scale-down: the database is the same one.
        expect((yield* untilAnswered<{ value: number }>(`/credit/${id}/tx1/3`, 120)).value).toBe(3);
      }),
      { timeout: 1_800_000, exclusive: railwayApi },
    );
  },
);
