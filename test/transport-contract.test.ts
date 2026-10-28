import { transportContract } from "./contracts/transport.js";
import {
  createInMemoryBroker,
  createInMemoryTransport,
} from "../src/transport/memory.js";

transportContract("memory", () => {
  const broker = createInMemoryBroker();
  return async () => createInMemoryTransport({ broker });
});
