import { afterEach } from "vitest";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { eventPageContract } from "./contracts/event-pages.js";
const stores: ReturnType<typeof createInMemoryEventStore>[] = [];
afterEach(async () => {
  await Promise.all(stores.splice(0).map((store) => store.close()));
});
eventPageContract("memory", async () => {
  const store = createInMemoryEventStore();
  stores.push(store);
  return store;
});
