import { expect, test } from "vitest";
import {
  comparePositions,
  decodePosition,
  encodePosition,
} from "../src/position.js";
test("Position tokens round-trip without losing xid8 precision", () => {
  const position = { transactionId: "18446744073709551614", sequence: "91" };
  expect(decodePosition(encodePosition(position))).toEqual(position);
  expect(
    comparePositions(position, {
      transactionId: "9007199254740992",
      sequence: "999",
    }),
  ).toBe(1);
  expect(() => decodePosition("malformed")).toThrow("Invalid Position");
});
