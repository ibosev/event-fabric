import type { Request } from "express";
import { Rejection } from "../errors.js";
import { decodePosition } from "../position.js";

/** The client's read-your-writes Position, from the header or the `minPosition` query parameter. */
export const minPosition = (req: Request) => {
  const token = req.get("EventFabric-Min-Position") ?? req.query.minPosition;
  if (token === undefined) return undefined;
  if (typeof token !== "string")
    throw new Rejection("validation", "Invalid Position token");
  try {
    return decodePosition(token);
  } catch {
    throw new Rejection("validation", "Invalid Position token");
  }
};
