import * as Railway from "@alchemy.run/railway";
import type { Input } from "alchemy/Input";
import * as Output from "alchemy/Output";
import type { Stack } from "alchemy/Stack";
import * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type { Database } from "../Database.ts";
import { GATEWAY_PORT } from "../internal/Gateway.ts";
import { DatabaseNetworkMismatch, makePoolLayer, type PoolSettings } from "../internal/PoolHost.ts";
import type { PoolHost } from "../internal/PoolHostService.ts";
import type { Providers } from "../Providers.ts";

/** How long Railway lets a replica's runner stop before killing it. */
const RUNNER_DRAINING_SECONDS = 30;

export interface ServicePoolOptions extends PoolSettings {
  /**
   * Region of the runner.
   * @default the workspace's default region
   */
  region?: string;
  /**
   * The Railway project the runner runs in when the database is in none,
   * such as `Cluster.Database.fromUrl`. Ignored when the database is in one.
   */
  project?: Input<Railway.ServiceProps["project"]>;
  /**
   * The environment of `project` the runner runs in.
   * @default the project's primary environment
   */
  environment?: Input<Railway.ServiceProps["environment"]>;
  /**
   * How many runners serve the pool, as replicas of its one Railway
   * Service. They share the pool's shard group; the Service's private
   * hostname stays the gateway callers reach, and lists every replica.
   * Changing it starts a deployment that replaces every replica (Railway
   * reuses the built image), and each runner it stops hands its shards
   * back first. Needs the database on the project's private network
   * (`ClusterRailway.Database`).
   * @default the Service's current count: 1 when it is created
   */
  replicas?: number;
}

/**
 * Run a pool of Effect Cluster runners on Railway: one Railway Service in
 * the database's project and environment, built from the bundled runner
 * program, with as many runners as it has `replicas`. Callers in the same
 * environment reach the pool's gateway at `{name}.railway.internal` on the
 * project's private network: it has no public domain. With a database in no
 * Railway project (`Cluster.Database.fromUrl`), give the pool its `project`.
 *
 * Beside a `ClusterRailway.Database`, each runner registers under its own
 * private address, so replicas, and a deployment overlapping the one it
 * replaces, never share one. Railway stops a runner with SIGTERM on every
 * deploy, restart and scale; it hands its shards back and unregisters, so
 * the other runners take them within seconds. A runner that crashes leaves
 * the shard assignments at the next health check, every 5 seconds. With a
 * database elsewhere, the pool runs one runner under the Service's hostname.
 *
 * ### Running on Railway
 * **Example:** A pool beside its database
 * ```typescript
 * export const Project = Railway.Project("App");
 *
 * export const Database = ClusterRailway.Database("Cluster", { project: Project });
 *
 * export const Default = ClusterRailway.ServicePool("Default").pipe(Layer.provide(Database));
 * ```
 *
 * **Example:** Three runners in one shard group
 * ```typescript
 * export const Default = ClusterRailway.ServicePool("Default", {
 *   replicas: 3,
 * }).pipe(Layer.provide(Database));
 * ```
 *
 * **Example:** A pool on a database you already run
 * ```typescript
 * export const Database = Cluster.Database.fromUrl({
 *   url: Config.Redacted("CLUSTER_DATABASE_URL"),
 * });
 *
 * export const Default = ClusterRailway.ServicePool("Default", {
 *   project: Project,
 *   region: "us-west2",
 * }).pipe(Layer.provide(Database));
 * ```
 *
 * @primitive
 */
export const ServicePool = (
  id: string,
  options: ServicePoolOptions = {},
): Layer.Layer<PoolHost, never, Database | Stack | Providers | Railway.Providers> =>
  makePoolLayer({
    label: "ClusterRailway.ServicePool",
    id,
    settings: options,
    expected: "a Railway project (ClusterRailway.Database)",
    networkOption: "project",
    // A private hostname is one DNS label.
    nameLength: 63,
    network: (database) => (database.network?.kind === "railway" ? database.network : undefined),
    ownNetwork: Effect.succeed(
      options.project === undefined
        ? undefined
        : { kind: "railway" as const, project: options.project, environment: options.environment },
    ),
    // Callers deploy before the runner exists, when a Railway reference to
    // it would render empty. The Service is created with this name, which is
    // its private hostname.
    gateway: (name) => Effect.succeed({ url: `http://${name}.railway.internal:${GATEWAY_PORT}` }),
    nodes: ({ database, network, name, program, environment }) => {
      // On the project's private network a runner has an address of its
      // own: the one it reaches the database from. With a database
      // elsewhere it only has the Service's hostname, which every replica
      // and an overlapping deployment's runner would share.
      const ownAddress = database.network?.kind === "railway";
      if (options.replicas !== undefined && !ownAddress) {
        return Effect.die(
          new DatabaseNetworkMismatch({
            message:
              "ClusterRailway.ServicePool runs `replicas` beside a database on the project's private network (ClusterRailway.Database).",
          }),
        );
      }
      return Railway.Service(`${id}Pool`, {
        project: network.project as Input<Railway.ServiceProps["project"]>,
        ...(network.environment === undefined
          ? {}
          : { environment: network.environment as Input<Railway.ServiceProps["environment"]> }),
        name,
        // The bundled runner and its Dockerfile, uploaded as the build context.
        context: program.pipe(Output.map((p) => p.dir)),
        port: GATEWAY_PORT,
        healthcheck: "/health",
        publicDomain: false,
        ...(options.region === undefined ? {} : { region: options.region }),
        ...(options.replicas === undefined ? {} : { replicas: options.replicas }),
        // Time between SIGTERM and SIGKILL, for a stopping runner to hand
        // its shards back (CLUSTER_SHUTDOWN_TIMEOUT_MS, below).
        ...(ownAddress ? { drainingSeconds: RUNNER_DRAINING_SECONDS } : {}),
        env: environment.pipe(
          Output.map((env) => ({
            ...env,
            // Railway's private network is IPv6: listen on every address.
            CLUSTER_LISTEN_HOST: "::",
            // Railway keeps a variable a deploy no longer sets: set every
            // one, so switching between the two never inherits the other's.
            ...(ownAddress
              ? {
                  CLUSTER_RUNNER_HOST: "",
                  CLUSTER_RUNNER_ROUTE: "database",
                  // Railway stops a runner with SIGTERM on every deploy,
                  // restart and scale: it hands its shards back and
                  // unregisters, so the others take them at once.
                  CLUSTER_SHUTDOWN_TIMEOUT_MS: String((RUNNER_DRAINING_SECONDS - 5) * 1000),
                  // Effect Cluster's own setting: check the other runners
                  // every 5 s, so one that crashed leaves the assignments
                  // within seconds, not when its registration expires.
                  RUNNER_HEALTH_CHECK_INTERVAL: "5 seconds",
                }
              : {
                  // The one runner registers under the hostname the others resolve.
                  CLUSTER_RUNNER_HOST: "${{RAILWAY_PRIVATE_DOMAIN}}",
                  CLUSTER_RUNNER_ROUTE: "",
                  CLUSTER_SHUTDOWN_TIMEOUT_MS: "",
                }),
          })),
        ),
      }).pipe(Effect.asVoid);
    },
  });
