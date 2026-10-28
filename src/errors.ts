export class ConcurrencyError extends Error {
  override name = "ConcurrencyError";
}
export class LeaseLostError extends Error {
  override name = "LeaseLostError";
}
// A Saga Message handled after it was replaced, cancelled or handled elsewhere.
export class StaleSagaMessageError extends Error {
  override name = "StaleSagaMessageError";
  constructor() {
    super("Saga Message is no longer pending");
  }
}
// A Readmodel write under an older Lease epoch than the store has seen.
export class StaleEpochError extends Error {
  override name = "StaleEpochError";
  constructor() {
    super("Stale Readmodel epoch");
  }
}
// A rebuild generation asked to swap in before it reached the live Checkpoint.
/** A live batch was folded against a generation that is no longer active; fold it again. */
export class StaleGenerationError extends Error {
  override name = "StaleGenerationError";
  constructor() {
    super("Readmodel generation changed while the batch was folded");
  }
}
export class GenerationBehindError extends Error {
  override name = "GenerationBehindError";
  constructor() {
    super("Readmodel generation has not caught up");
  }
}
// A rebuild of a Readmodel group that another instance is already running.
export class RebuildRunningError extends Error {
  override name = "RebuildRunningError";
  constructor() {
    super("A rebuild is already running for this Readmodel group");
  }
}
// A read of a Readmodel group whose required rebuild has not swapped in yet.
export class ReadmodelRebuildingError extends Error {
  override name = "ReadmodelRebuildingError";
  code = "readmodel-rebuilding";
  constructor(public group: string) {
    super(`Readmodel group ${group} is rebuilding; retry after it swaps in`);
  }
}
// A Command id that another append already recorded as accepted.
export class DuplicateCommandError extends Error {
  override name = "DuplicateCommandError";
  constructor() {
    super("Command already accepted");
  }
}
export class Rejection extends Error {
  override name = "Rejection";
  /** The Application error a Step threw, as JSON: its `name` and own fields, without the stack. */
  thrown?: Record<string, unknown>;
  constructor(
    public kind: string,
    message: string,
    public details?: any,
    public code: string = kind,
  ) {
    super(message);
  }
}
export class ForbiddenError extends Error {
  override name = "ForbiddenError";
  code = "forbidden";
  constructor() {
    super("Forbidden");
  }
}
// Thrown into a Workflow body at the next ctx await after the instance was cancelled.
export class WorkflowCancelled extends Error {
  override name = "WorkflowCancelled";
}
// A Workflow ctx call after the run was judged suspended: the body awaited something other than a ctx call.
export class LateWorkflowCallError extends Error {
  override name = "LateWorkflowCallError";
}
