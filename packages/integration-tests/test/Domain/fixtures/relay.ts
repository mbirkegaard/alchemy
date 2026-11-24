/**
 * A TCP relay to a local server that a test can cut: `cut` closes every
 * connection through it at once, as a server replaced mid-response does,
 * and later connections go through again.
 */
import { createServer, connect, type Socket } from "node:net";
import * as Effect from "effect/Effect";

export interface Relay {
  readonly port: number;
  /** Close every connection open through the relay now. */
  readonly cut: Effect.Effect<number>;
}

export const relayTo = (port: number) =>
  Effect.acquireRelease(
    Effect.callback<{ relay: Relay; close: () => void }>((resume) => {
      const open = new Set<Socket>();
      const server = createServer((client) => {
        const upstream = connect({ host: "127.0.0.1", port });
        open.add(client);
        open.add(upstream);
        client.pipe(upstream);
        upstream.pipe(client);
        const close = () => {
          client.destroy();
          upstream.destroy();
          open.delete(client);
          open.delete(upstream);
        };
        client.on("error", close).on("close", close);
        upstream.on("error", close).on("close", close);
      });
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        resume(
          Effect.succeed({
            relay: {
              port: typeof address === "object" && address !== null ? address.port : 0,
              cut: Effect.sync(() => {
                const count = open.size;
                for (const socket of open) socket.destroy();
                open.clear();
                return count;
              }),
            },
            close: () => {
              for (const socket of open) socket.destroy();
              server.close();
            },
          }),
        );
      });
    }),
    ({ close }) => Effect.sync(close),
  ).pipe(Effect.map(({ relay }) => relay));
