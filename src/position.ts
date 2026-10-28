import type { Position } from "./types.js";
export const comparePositions = (a: Position, b: Position): number => {
  for (const key of ["transactionId", "sequence"] as const) {
    const x = BigInt(a[key]),
      y = BigInt(b[key]);
    if (x !== y) return x < y ? -1 : 1;
  }
  return 0;
};
export const encodePosition = (position: Position): string =>
  Buffer.from(`${position.transactionId}:${position.sequence}`).toString(
    "base64url",
  );
export const decodePosition = (token: string): Position => {
  if (!/^[A-Za-z0-9_-]+$/.test(token))
    throw new Error("Invalid Position token");
  const decoded = Buffer.from(token, "base64url").toString();
  const match = /^(0|[1-9]\d*):(0|[1-9]\d*)$/.exec(decoded);
  if (!match || Buffer.from(decoded).toString("base64url") !== token)
    throw new Error("Invalid Position token");
  return { transactionId: match[1]!, sequence: match[2]! };
};
