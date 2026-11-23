/**
 * The Railway suite's front: a public Railway Service in the cluster's
 * environment, calling everything through the gateways on the private network.
 */
import * as Railway from "@alchemy.run/railway";
import { frontApp } from "../app.ts";
import { Partition, Project } from "./cluster.ts";
import { Tally } from "./tally.ts";

export default class Front extends Railway.Service<Front>()(
  "ClusterRailwayFront",
  {
    project: Project,
    environment: Partition,
    main: import.meta.url,
    port: 3000,
    // Use `/ready` instead of Railway's default `/health` probe.
    healthcheck: "/ready",
  },
  frontApp(Tally),
) {}
