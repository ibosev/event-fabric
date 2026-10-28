import type { MongoClient } from "mongodb";

/** Drops the per-group databases the Mongo Readmodel store creates next to each test's records database. */
export const dropGroupDatabases = async (mongo: MongoClient) => {
  const { databases } = await mongo.db().admin().listDatabases();
  for (const { name } of databases)
    if (name.endsWith("-readmodels")) await mongo.db(name).dropDatabase();
};
