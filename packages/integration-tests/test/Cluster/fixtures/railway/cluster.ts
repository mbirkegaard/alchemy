/**
 * The Railway suite's cluster: Railway Postgres and a runner Service per
 * pool, in an environment of the shared suite project.
 */
import * as ClusterRailway from "@alchemy.run/effect-cluster/Railway";
import { Environment } from "@alchemy.run/railway/ProjectEnvironment";
import * as Layer from "effect/Layer";
import { suiteProject } from "../../../Railway/suiteProject.ts";

/** The shared suite project: not a stack resource, so destroy keeps it. */
export const Project = suiteProject;

/** The suite's own environment, which destroy deletes. */
export const Partition = Environment("ClusterPartition", { project: Project });

export const Database = ClusterRailway.Database("Cluster", {
  project: Project,
  environment: Partition,
});

export const Default = ClusterRailway.ServicePool("Default").pipe(Layer.provide(Database));

/** A pool of its own for the external program. */
export const Legacy = ClusterRailway.ServicePool("Legacy").pipe(Layer.provide(Database));
