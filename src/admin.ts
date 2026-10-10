/** SQL for the single parameterized call used for Db2 administrative commands. */
export const adminCommandCallSql = "CALL SYSPROC.ADMIN_CMD(?)";

/** Normalizes and validates a command before it is sent as a bound parameter. */
export function normalizeAdminCommand(command: string): string {
  if (typeof command !== "string") throw new TypeError("DB2 admin command must be a string");
  if (command.includes("\0")) throw new TypeError("DB2 admin command cannot contain a NUL character");
  const normalized = command.trim();
  if (normalized.length === 0) throw new TypeError("DB2 admin command cannot be empty");
  return normalized;
}
