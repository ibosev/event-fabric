import { comparePositions } from "../position.js";
import type { EventPage, EventStore, ReadPageOptions } from "../types.js";
import { assertEventNames } from "./upcasters.js";

export function pageLimit(options: ReadPageOptions): number {
  assertEventNames(options.eventNames);
  const limit = options.limit ?? 1000;
  if (
    !Number.isSafeInteger(limit) ||
    limit < 1 ||
    limit >= Number.MAX_SAFE_INTEGER
  )
    throw new Error("Event page limit must be a positive safe integer");
  for (const position of [options.fromPosition, options.toPosition])
    if (
      position &&
      [position.transactionId, position.sequence].some(
        (value) => typeof value !== "string" || !/^(0|[1-9]\d*)$/.test(value),
      )
    )
      throw new Error("Invalid Event page Position");
  if (
    options.fromPosition &&
    options.toPosition &&
    comparePositions(options.fromPosition, options.toPosition) > 0
  )
    throw new Error("Event page fromPosition must not exceed toPosition");
  return limit;
}

/** Older adapters retain safe progress: choose the ceiling before reading, never after an empty page. */
export async function readEventPage(
  store: Pick<EventStore, "head" | "readAll" | "readPage">,
  options: ReadPageOptions = {},
): Promise<EventPage> {
  const limit = pageLimit(options);
  if (store.readPage) return store.readPage(options);
  const head = await store.head();
  const through =
    head && options.toPosition && comparePositions(options.toPosition, head) < 0
      ? options.toPosition
      : head;
  if (
    !through ||
    (options.fromPosition &&
      comparePositions(through, options.fromPosition) <= 0)
  )
    return { events: [], through: options.fromPosition ?? null, complete: true };
  const events = await store.readAll({
    ...options,
    toPosition: through,
    limit,
  });
  return {
    events,
    through: events.length === limit ? events.at(-1)!.position : through,
    complete: events.length < limit,
  };
}

/** Whether a page reached the end of the visible log (or its `toPosition`). */
export const pageComplete = (page: EventPage, limit: number) =>
  page.complete ?? page.events.length < limit;
