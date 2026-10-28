/*
 * A rebuild worker thread: loads the Application from its configuration module, as the CLI does.
 * It either rebuilds one whole group, or is one fold worker of a sliced rebuild
 * and folds the pages the rebuild shares with it.
 */
import { parentPort, workerData } from "node:worker_threads";
import { resolveComponents } from "../authoring/decorators.js";
import type { EventPage } from "../types.js";
import { createApplication } from "./index.js";
import { foldSlice } from "../internal.js";

const { module, task, rebuild, rebuildCacheBytes } = workerData;
const loaded = await import(module);
const application = createApplication({
  ...resolveComponents(
    typeof loaded.config === "function"
      ? await loaded.config(process.env.EVENT_FABRIC_ENV)
      : loaded.config,
  ),
  rebuildCacheBytes,
});
// One group's whole rebuild, for `rebuildGroups`.
if (rebuild) {
  const controller = new AbortController();
  parentPort!.on("message", (message) => {
    if (message === "abort") controller.abort();
  });
  try {
    parentPort!.postMessage({
      result: await application.rebuild(rebuild.name, {
        swap: rebuild.swap,
        signal: controller.signal,
      }),
    });
  } catch (error: any) {
    parentPort!.postMessage({
      error: {
        name: error?.name,
        message: String(error?.message ?? error),
        stack: error?.stack,
        eventId: error?.eventId ?? error?.cause?.eventId,
      },
    });
  } finally {
    await application.close();
  }
  parentPort!.close();
} else {
  const queue: (EventPage | undefined)[] = [];
  let wake = () => {};
  parentPort!.on("message", (message: EventPage | "finish") => {
    queue.push(message === "finish" ? undefined : message);
    wake();
  });
  const pages = async () => {
    while (!queue.length)
      await new Promise<void>((resolve) => (wake = resolve));
    return queue.shift();
  };
  try {
    await application[foldSlice](task, pages, () =>
      parentPort!.postMessage("folded"),
    );
  } finally {
    await application.close();
  }
  parentPort!.postMessage("done");
  parentPort!.close();
}
