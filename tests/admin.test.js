const test = require('node:test');
const assert = require('node:assert/strict');
const { adminCommandCallSql, normalizeAdminCommand } = require('../dist/cjs/admin.js');

test('normalizes an administrative command and leaves its content intact for binding', () => {
  assert.equal(adminCommandCallSql, 'CALL SYSPROC.ADMIN_CMD(?)');
  assert.equal(normalizeAdminCommand("  RUNSTATS ON TABLE S.T WITH DISTRIBUTION  "), 'RUNSTATS ON TABLE S.T WITH DISTRIBUTION');
  assert.equal(normalizeAdminCommand("COMMENT ON TABLE S.T IS 'O''Reilly'"), "COMMENT ON TABLE S.T IS 'O''Reilly'");
});

test('rejects blank, non-string, and NUL-containing administrative commands', () => {
  assert.throws(() => normalizeAdminCommand(' \t\r\n'), /cannot be empty/);
  assert.throws(() => normalizeAdminCommand(null), /must be a string/);
  assert.throws(() => normalizeAdminCommand('RUNSTATS\0 ON TABLE S.T'), /NUL/);
});
