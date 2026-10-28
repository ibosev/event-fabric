import type { Stop, Transport } from "../types.js";
type Handler = (message: any) => void | Promise<void>;
/**
 * One Transport subscription per topic for the whole process, fanned out to every local subscriber:
 * a thousand live clients cost one broker queue and one parse per message, not a thousand. Every
 * subscriber receives the same message object and must not change it.
 */
export function sharedSubscriptions(
  transport: Pick<Transport, "subscribe">,
): Pick<Transport, "subscribe"> {
  const topics = new Map<
    string,
    { handlers: Set<Handler>; stop: Promise<Stop> }
  >();
  return {
    async subscribe(topic, handler) {
      // Each subscription is its own entry, even for the same function.
      const own: Handler = (message) => handler(message);
      let entry = topics.get(topic);
      if (!entry) {
        const handlers = new Set<Handler>();
        const created = {
          handlers,
          stop: transport.subscribe(topic, (message) =>
            Promise.allSettled(
              [...handlers].map((h) =>
                Promise.resolve().then(() => h(message)),
              ),
            ).then(() => {}),
          ),
        };
        entry = created;
        topics.set(topic, created);
        created.stop.catch(() => {
          if (topics.get(topic) === created) topics.delete(topic);
        });
      }
      entry.handlers.add(own);
      try {
        await entry.stop;
      } catch (error) {
        entry.handlers.delete(own);
        throw error;
      }
      const current = entry;
      let stopped = false;
      return async () => {
        if (stopped) return;
        stopped = true;
        current.handlers.delete(own);
        if (current.handlers.size || topics.get(topic) !== current) return;
        topics.delete(topic);
        await (
          await current.stop
        )();
      };
    },
  };
}
