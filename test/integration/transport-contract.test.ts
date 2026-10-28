import { transportContract } from "../contracts/transport.js";
import { createRabbitMQTransport } from "../../src/rabbitmq.js";

transportContract("RabbitMQ", () => {
  const application = `contract-${crypto.randomUUID()}`;
  return () =>
    createRabbitMQTransport({ application, url: "amqp://127.0.0.1:55679" });
});
