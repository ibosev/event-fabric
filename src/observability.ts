import pino from "pino";
import {
  context,
  propagation,
  trace,
  metrics,
  SpanStatusCode,
} from "@opentelemetry/api";
import type { Logger, StreamId } from "./types.js";
/** pino at `info` by default; pass `{ level: "debug" }` for the diagnostic summaries. */
export const createLogger = (options: pino.LoggerOptions = {}): Logger =>
  pino({ name: "event-fabric", ...options });
/** A Stream as one searchable log value, `context.aggregate.id`. */
export const streamName = (stream: StreamId) =>
  `${stream.context}.${stream.aggregate}.${stream.id}`;
/** A Command's or Event's qualified type name, `context.aggregate.name`. */
export const qualifiedName = ({ stream, name }: { stream: Omit<StreamId, "id">; name: string }) =>
  `${stream.context}.${stream.aggregate}.${name}`;
/** What an operator runs next, as the `recovery` field of the records that need one. */
export const recovery = {
  deadLetter: (id: string) =>
    `fix the cause, then event-fabric dead-letters replay ${id} (or discard ${id})`,
  halted: (group: string, eventId?: string) =>
    `fix the cause, then event-fabric consumers retry ${group}${eventId ? ` (or consumers skip ${group} --event ${eventId})` : ""}`,
};
/**
 * Sums durations and counts per key (a Consumer group, a Readmodel group) and logs each key's
 * totals at `debug` on `flush`. Disabled loggers skip timing and accumulation.
 */
export const createTimings = (logger: Logger, message: string, keyName: string) => {
  let window = new Map<string, Record<string, number>>(),
    since = performance.now();
  const add = (key: string, field: string, value = 1) => {
    if (logger.isLevelEnabled?.("debug") === false) return;
    const fields = window.get(key) ?? {};
    window.set(key, fields);
    fields[field] = (fields[field] ?? 0) + value;
  };
  return {
    add,
    /** Adds the work's duration to `<stage>Ms` and counts it in `<stage>Count`. */
    time<T>(key: string, stage: string, work: () => Promise<T>): Promise<T> {
      if (logger.isLevelEnabled?.("debug") === false) return work();
      const start = performance.now();
      return (async () => {
        try {
          return await work();
        } finally {
          add(key, `${stage}Ms`, performance.now() - start);
          add(key, `${stage}Count`);
        }
      })();
    },
    flush() {
      const ended = window,
        windowMs = Math.round(performance.now() - since);
      window = new Map();
      since = performance.now();
      if (logger.isLevelEnabled?.("debug") === false) return;
      for (const [key, fields] of ended)
        logger.debug(
          {
            [keyName]: key,
            windowMs,
            ...Object.fromEntries(
              Object.entries(fields).map(([field, value]) => [field, Math.round(value)]),
            ),
          },
          message,
        );
    },
  };
};
export type Timings = ReturnType<typeof createTimings>;
const meter = metrics.getMeter("event-fabric");
const counters = new Map<string, ReturnType<typeof meter.createCounter>>();
const histograms = new Map<string, ReturnType<typeof meter.createHistogram>>();
export const count = (
  name: string,
  attributes: Record<string, string | number> = {},
  value = 1,
): void => {
  if (!counters.has(name)) counters.set(name, meter.createCounter(name));
  counters.get(name)!.add(value, attributes);
};
export const measure = (
  name: string,
  value: number,
  attributes: Record<string, string | number> = {},
): void => {
  if (!histograms.has(name)) histograms.set(name, meter.createHistogram(name));
  histograms.get(name)!.record(value, attributes);
};
export const gauge = (
  name: string,
  observe: () => {
    value: number;
    attributes?: Record<string, string | number>;
  }[],
): (() => void) => {
  const instrument = meter.createObservableGauge(name);
  const callback = (result: import("@opentelemetry/api").ObservableResult) => {
    for (const entry of observe())
      result.observe(entry.value, entry.attributes);
  };
  instrument.addCallback(callback);
  return () => instrument.removeCallback(callback);
};
export async function withSpan<T>(
  name: string,
  envelope: { correlationId?: string; traceparent?: string },
  fn: () => Promise<T>,
  linked = false,
): Promise<T> {
  const parent = propagation.extract(
    context.active(),
    envelope.traceparent ? { traceparent: envelope.traceparent } : {},
  );
  const link = trace.getSpanContext(parent);
  return trace
    .getTracer("event-fabric")
    .startActiveSpan(
      name,
      {
        attributes: {
          "eventfabric.correlation_id": envelope.correlationId ?? "",
        },
        ...(linked && link ? { links: [{ context: link }] } : {}),
      },
      linked ? context.active() : parent,
      async (span) => {
        try {
          return await fn();
        } catch (error) {
          span.setStatus({ code: SpanStatusCode.ERROR });
          throw error;
        } finally {
          span.end();
        }
      },
    );
}
export const traceparent = (): string | undefined => {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier.traceparent;
};
