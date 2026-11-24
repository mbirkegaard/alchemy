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
   * Number of runners, as replicas of the one Railway Service. Each
   * registers under its own private address, the one it reaches the
   * database from, so they serve one shard group; the Service's private
   * hostname stays the gateway callers reach. Needs the database on the
   * project's private network (`ClusterRailway.Database`). A replica that
   * Railway stops hands its shards back before it exits. Setting it, even to 1, also
   * keeps an overlapping deployment's runner distinct from the one it
   * replaces.
   * @default one runner, registered under the Service's private hostname
   */
  replicas?: number;
}

/**
 * Run a pool of Effect Cluster runners on Railway: one Railway Service in
 * the database's project and environment, built from the bundled runner
 * program. The runner registers under `{name}.railway.internal` on the
 * project's private network, where callers in the same environment reach
 * its gateway too: the gateway has no public domain. With a database in no
 * Railway project (`Cluster.Database.fromUrl`), give the pool its `project`.
 *
 * Railway gives every replica of a Service the same private hostname, so by
 * default the pool runs one runner under it. With `replicas`, each replica
 * registers under its own private address instead, and they share the
 * pool's shard group.
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
    nodes: ({ database, network, name, program, environment }) =>
      // Replicas find their own address by their route to the database,
      // which only names the private network when the database is on it.
      options.replicas !== undefined && database.network?.kind !== "railway"
        ? Effect.die(
            new DatabaseNetworkMismatch({
              message:
                "ClusterRailway.ServicePool runs `replicas` beside a database on the project's private network (ClusterRailway.Database).",
            }),
          )
        : Railway.Service(`${id}Pool`, {
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
            ...(options.replicas === undefined
              ? {}
              : {
                  replicas: options.replicas,
                  // Time between SIGTERM and SIGKILL, for a stopping runner to
                  // hand its shards back (CLUSTER_SHUTDOWN_TIMEOUT_MS, below).
                  drainingSeconds: RUNNER_DRAINING_SECONDS,
                }),
            env: environment.pipe(
              Output.map((env) => ({
                ...env,
                // Railway's private network is IPv6: listen on every address.
                CLUSTER_LISTEN_HOST: "::",
                ...(options.replicas === undefined
                  ? // One runner registers under the hostname the others resolve.
                    { CLUSTER_RUNNER_HOST: "${{RAILWAY_PRIVATE_DOMAIN}}" }
                  : // Every replica shares that hostname, which lists none of
                    // them until their deployment is live: each registers under
                    // its own private address, the one it reaches the database from.
                    {
                      CLUSTER_RUNNER_ROUTE: "database",
                      CLUSTER_SHUTDOWN_TIMEOUT_MS: String((RUNNER_DRAINING_SECONDS - 5) * 1000),
                    }),
              })),
            ),
          }).pipe(Effect.asVoid),
  });
