import { createInterface } from "node:readline";
/**
 * Sends requests the way one runner sends another, through RunnerTransport's
 * client, as its own process: reads a URL per line on stdin, GETs it, and
 * prints one JSON line per answer: `{ url, ok, ms, error? }`.
 */
import { runnerHttpClient } from "@alchemy.run/effect-cluster/internal/RunnerTransport";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as HttpClient from "effect/http/HttpClient";
import * as ManagedRuntime from "effect/ManagedRuntime";

const runtime = ManagedRuntime.make(runnerHttpClient);

const get = (url: string) =>
  Effect.gen(function* () {
    const started = Date.now();
    const client = yield* HttpClient.HttpClient;
    const exit = yield* Effect.exit(Effect.flatMap(client.get(url), (response) => response.text));
    return {
      url,
      ok: Exit.isSuccess(exit),
      ms: Date.now() - started,
      ...(Exit.isFailure(exit) ? { error: String(exit.cause) } : {}),
    };
  });

createInterface({ input: process.stdin }).on("line", (url) => {
  void runtime.runPromise(get(url)).then((result) => console.log(JSON.stringify(result)));
});
