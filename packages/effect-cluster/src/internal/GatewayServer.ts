import { createServer } from "node:http";
/**
 * The HTTP server a runner's gateway listens on (Node only: Workers serve a
 * gateway through their own `fetch`).
 */
import { NodeHttpServer } from "@effect/platform-node";
import * as Duration from "effect/Duration";
import { GATEWAY_PORT } from "./Gateway.ts";

/**
 * How long a stopping gateway lets requests in flight finish: 2 seconds,
 * not Node's 20. The runner holds its shards until its gateway has stopped,
 * and answers nothing on them in the meantime, so one request left open
 * through the gateway held the runner's keys unanswered for 20 seconds.
 */
export const GATEWAY_DRAIN = Duration.seconds(2);

/** The gateway's HTTP server, on `port` (the gateway port by default). */
export const gatewayHttpServer = (port: number = GATEWAY_PORT) =>
  NodeHttpServer.layer(createServer, { port, gracefulShutdownTimeout: GATEWAY_DRAIN });
