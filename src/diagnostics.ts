import { channel } from "node:diagnostics_channel";

export type Db2Diagnostic = { operation: "acquire" | "connect" | "execute" | "reset" | "discard" | "spool"; durationMs: number; success: boolean; bytes?: number };
/** Metadata-only channel. Use observeDb2Diagnostics to isolate observer exceptions. */
export const db2DiagnosticsChannel = channel("justybase.db2");
export function publishDb2Diagnostic(event: Db2Diagnostic): void {
  if (db2DiagnosticsChannel.hasSubscribers) db2DiagnosticsChannel.publish(Object.freeze({ ...event }));
}
/** Safe subscription: observer exceptions never affect driver operations. */
export function observeDb2Diagnostics(observer: (event: Readonly<Db2Diagnostic>) => void): () => void {
  const subscriber = (event: unknown) => { try { observer(event as Readonly<Db2Diagnostic>); } catch { /* diagnostic only */ } };
  db2DiagnosticsChannel.subscribe(subscriber);
  return () => db2DiagnosticsChannel.unsubscribe(subscriber);
}
