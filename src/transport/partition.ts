import { createHash } from "node:crypto";
import type { Command, StreamId } from "../types.js";
export const fnv1a = (key: string): number => {
  let hash = 0x811c9dc5;
  for (const byte of Buffer.from(key, "utf8"))
    hash = Math.imul(hash ^ byte, 0x01000193);
  return hash >>> 0;
};
export const partitionFor = (stream: StreamId): number =>
  fnv1a(`${stream.context}.${stream.aggregate}.${stream.id}`) % 128;
/**
 * A Command's Partition. A creation Command without a Stream id has no Stream to keep
 * in order yet, so it goes by its Command id, which a resend keeps: otherwise every
 * creation of an Aggregate type would share one Partition.
 */
export const commandPartition = (command: Command): number =>
  command.stream.id ? partitionFor(command.stream) : fnv1a(command.id) % 128;
/** The Partition that owns a document or Saga instance in a group of `count` Partitions. */
export const keyPartition = (key: string, count: number): number =>
  fnv1a(key) % count;
/**
 * An instance's score for one key (rendezvous hashing): among the live instances, the
 * highest score owns the key. Keys spread evenly, and an instance joining or leaving
 * moves only its own share.
 */
export const rendezvous = (instance: string, key: string): number =>
  // SHA-256, not FNV-1a: FNV-1a mixes similar keys too weakly for a fair spread.
  createHash("sha256").update(`${instance}/${key}`).digest().readUInt32BE(0) &
  0x7fffffff;
/** An instance's consumer priority on one Command Partition: the highest live one is its active handler. */
export const commandPriority = (
  instance: string,
  context: string,
  partition: number,
): number => rendezvous(instance, `${context}/${partition}`);
