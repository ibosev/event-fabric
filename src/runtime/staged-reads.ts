import { comparePositions } from "../position.js";
import { queryDocuments, textFields } from "../readmodel-store/query.js";
import { normalizeDocument, resolveSchema } from "../readmodel-store/schema.js";
import type { Filter, ReadmodelChange } from "../readmodel-store/types.js";
import type { JsonObject, Position, Schema } from "../types.js";
import type { ReadOptions } from "./readmodels.js";
import type { ReadmodelGroup } from "./types.js";

/**
 * Repositories over a Readmodel group as the batch in progress sees it: the
 * stored generation overlaid with the changes staged so far.
 */
export function stagedReads(
  name: string,
  group: ReadmodelGroup,
  schemas: Record<string, Schema> | undefined,
  changes: Map<string, ReadmodelChange>,
  checkpoint: Position | null,
  generation?: string,
) {
  // Each staged document normalized once per batch, not on every read: a fold stores a new object for its
  // change, so an entry is never reused for a later state. One folded in place keeps its pre-fold reading.
  const normalized = new WeakMap<JsonObject, JsonObject>();
  // Stored reads once per batch and filter: only this fold writes the generation, and its writes, including the
  // previous batch's carried changes still committing, are overlaid on every read.
  const stored = new Map<string, Promise<JsonObject[]>>();
  return Object.fromEntries(
    Object.entries(group.models).map(([model, definition]) => {
      const schema = resolveSchema(definition.schema, {
        ...schemas,
        ...group.schemas,
      });
      const find = async (filter: Filter = {}, options: ReadOptions = {}) => {
        if (
          options.minPosition &&
          (!checkpoint || comparePositions(checkpoint, options.minPosition) < 0)
        )
          throw new Error(
            "Cannot wait for uncommitted Readmodel data inside a Reaction",
          );
        const key = JSON.stringify([model, filter, options.q ?? null]);
        let read = stored.get(key);
        if (!read) {
          read = group.store.find(name, model, filter, {
            generation,
            q: options.q,
          });
          stored.set(key, read);
          read.catch(() => stored.delete(key));
        }
        const documents = new Map(
          (await read).map((doc) => [String(doc._id), doc]),
        );
        for (const change of changes.values()) {
          if (change.model !== model) continue;
          if (change.document) {
            let document = normalized.get(change.document);
            if (!document)
              normalized.set(
                change.document,
                (document = await normalizeDocument(
                  schema,
                  change.document,
                  change.id,
                  false,
                  undefined,
                  false,
                )),
              );
            documents.set(change.id, document);
          } else documents.delete(change.id);
        }
        // queryDocuments returns copies, so callers cannot change the cached documents.
        return queryDocuments(
          [...documents.values()],
          filter,
          options,
          textFields(definition),
        );
      };
      const findOne = async (filter: Filter = {}, options?: ReadOptions) =>
        (await find(filter, { ...options, size: 1 }))[0] ?? null;
      const read = async (id: string, options?: ReadOptions) => ({
        document: await findOne({ _id: id }, options),
        position: checkpoint,
        behind: false,
      });
      return [
        model,
        Object.assign(read, {
          find,
          findOne,
          count: async (filter: Filter = {}, options?: ReadOptions) =>
            (await find(filter, { ...options, from: 0, size: undefined }))
              .length,
          stream: async function* (filter: Filter = {}, options?: ReadOptions) {
            yield* await find(filter, options);
          },
          native: () => {
            throw new Error(
              "Native access cannot read staged Readmodel changes inside a Reaction",
            );
          },
        }),
      ];
    }),
  );
}
