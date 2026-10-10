const test = require("node:test");
const assert = require("node:assert/strict");
const { Db2CompatResult } = require("../dist/cjs/ibmDbCompat.js");

test("ibm_db migration cursor exposes metadata, object rows, fetchAll, and close", async () => {
  const columns = [
    { name: "ID", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: false },
    { name: "LABEL", sqlType: 449, length: 40n, precision: 40, scale: 0, nullable: true },
  ];
  let returned = false;
  async function* rows() {
    try {
      yield { columns, values: [7, "row"] };
      yield { columns, values: [8, null] };
    } finally { returned = true; }
  }

  const cursor = new Db2CompatResult(rows(), columns);
  assert.deepEqual(cursor.getColumnNames(), ["ID", "LABEL"]);
  assert.deepEqual(cursor.getColumnNamesSync(), ["ID", "LABEL"]);
  assert.equal(cursor.getColumnMetadata()[0].SQL_DESC_NAME, "ID");
  assert.equal(cursor.getColumnMetadataSync()[0].SQL_DESC_TYPE_NAME, "INTEGER");
  assert.equal(cursor.getColumnMetadata()[0].SQL_DESC_TYPE_NAME, "INTEGER");
  assert.deepEqual(await cursor.fetch(), { ID: 7, LABEL: "row" });
  assert.deepEqual(await cursor.fetchAll(), [{ ID: 8, LABEL: null }]);
  assert.equal(await cursor.fetch(), null);
  await cursor.close();
  assert.equal(returned, true);
});

test("JustyBase async executeSql shape and reader methods map to the native session", async () => {
  const { Db2CompatDatabase } = require("../dist/cjs/ibmDbCompat.js");
  const columns = [{ name: "ID", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: false }];
  const db = Object.create(Db2CompatDatabase.prototype);
  db.connection = {
    query: async () => ({ columns, rows: [[7]] }),
    execute: async (_sql, params) => params?.length ?? 3,
    prepare: async () => ({ outputColumns: columns, close() {} }),
    queryStream: async function* () { yield { columns, values: [9] }; },
    queryStreamWithParams: async function* () { yield { columns, values: [9] }; },
  };

  assert.deepEqual(await db.executeSql("SELECT 7 AS ID"), {
    columns,
    rows: [{ ID: 7 }],
    recordsAffected: -1,
  });
  assert.deepEqual(await db.executeSql("UPDATE T SET ID = ?", [7]), {
    columns: [], rows: [], recordsAffected: 1,
  });

  const reader = await db.queryResult("SELECT ID FROM T");
  assert.equal(reader.fieldCount, 1);
  assert.equal(reader.getName(0), "ID");
  assert.equal(reader.getTypeName(0), "INTEGER");
  assert.equal(await reader.read(), true);
  assert.equal(reader.getValue(0), 9);
  assert.equal(await reader.read(), false);
  await reader.close();
});

test("JustyBase command adapter supports reader execution, row counts, timeout mapping, and cancel", async () => {
  const { Db2CompatCommand, Db2CompatDatabase } = require("../dist/cjs/ibmDbCompat.js");
  const columns = [{ name: "ID", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: false }];
  let capturedOptions;
  const db = Object.create(Db2CompatDatabase.prototype);
  db.connection = {
    query: async (_sql, params, options) => {
      capturedOptions = options;
      return { columns, rows: [[params[0]]] };
    },
    execute: async (_sql, _params, options) => {
      capturedOptions = options;
      return 2;
    },
    prepare: async () => ({ outputColumns: columns, close() {} }),
    queryStream: async function* (_sql, options) {
      capturedOptions = options;
      yield { columns, values: [12] };
    },
    queryStreamWithParams: async function* (_sql, _params, options) {
      capturedOptions = options;
      yield { columns, values: [12] };
    },
  };
  const command = db.createCommand("SELECT ID FROM T WHERE ID = ?", [12]);
  command.commandTimeout = 3;
  const reader = await command.executeReader();
  assert.equal(command._recordsAffected, -1);
  assert.equal(await reader.read(), true);
  assert.equal(reader.getValue(0), 12);
  assert.equal(capturedOptions.timeoutMs, 3_000);
  await reader.close();

  const dml = new Db2CompatCommand(db, "UPDATE T SET ID = ?", [12]);
  const emptyReader = await dml.executeReader();
  assert.equal(dml._recordsAffected, 2);
  assert.equal(await emptyReader.read(), false);
  await dml.cancel();
  assert.equal(capturedOptions.signal.aborted, true);
});

test("JustyBase command routes CALL and advances result cursors", async () => {
  const { Db2CompatCommand, Db2CompatDatabase } = require("../dist/cjs/ibmDbCompat.js");
  const columns = [{ name: "VALUE", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: false }];
  let call;
  const db = Object.create(Db2CompatDatabase.prototype);
  db.connection = {};
  db.procedureResult = async (sql, params, options, onFinished) => {
    call = { sql, params, options };
    let index = 0;
    const procedure = {
      get columns() { return columns; },
      async *rows() { yield [index === 0 ? 1 : 2]; },
      async nextResultSet() { index += 1; return index === 1; },
      async finish() { onFinished(0); return { rowsAffected: 0, outputParameters: [] }; },
    };
    async function* noRows() {}
    return new (require("../dist/cjs/ibmDbCompat.js").Db2CompatResult)(noRows(), columns, procedure, onFinished);
  };
  const command = new Db2CompatCommand(db, "-- comment\nCALL P(?)", [7]);
  command.commandTimeout = 2;
  const reader = await command.executeReader();
  assert.equal(call.sql, "-- comment\nCALL P(?)");
  assert.deepEqual(call.params, [7]);
  assert.equal(call.options.timeoutMs, 2_000);
  assert.equal(await reader.read(), true);
  assert.equal(reader.getValue(0), 1);
  assert.equal(await reader.nextResult(), true);
  assert.equal(await reader.read(), true);
  assert.equal(reader.getValue(0), 2);
  assert.equal(await reader.nextResult(), false);
  assert.equal(await reader.read(), false);
  assert.equal(await reader.nextResult(), false);
  assert.equal(command._recordsAffected, 0);
  await reader.close();
});

test("closing an active ibm_db migration cursor closes its underlying row stream", async () => {
  let returned = false;
  async function* rows() {
    try { yield { columns: [], values: [] }; }
    finally { returned = true; }
  }
  const cursor = new Db2CompatResult(rows(), []);
  await cursor.fetch();
  await cursor.close();
  assert.equal(returned, true);
  assert.equal(await cursor.fetch(), null);
});

test("JustyBase reader adapter advances through streamed procedure result sets", async () => {
  const { Db2CompatResult } = require("../dist/cjs/ibmDbCompat.js");
  const firstColumns = [{ name: "FIRST", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: false }];
  const secondColumns = [{ name: "SECOND", sqlType: 449, length: 20n, precision: 20, scale: 0, nullable: true }];
  let setIndex = 0;
  let drained = false;
  const procedure = {
    get columns() { return setIndex === 0 ? firstColumns : secondColumns; },
    async *rows() {
      if (setIndex === 0) yield [11];
      else yield ["second set"];
    },
    async nextResultSet() { setIndex += 1; return setIndex === 1; },
    async finish() { drained = true; return { rowsAffected: 0, outputParameters: [] }; },
  };
  async function* noRows() {}
  const reader = new Db2CompatResult(noRows(), firstColumns, procedure);
  assert.equal(reader.getName(0), "FIRST");
  assert.equal(await reader.read(), true);
  assert.equal(reader.getValue(0), 11);
  assert.equal(await reader.read(), false);
  assert.equal(await reader.nextResult(), true);
  assert.equal(reader.getName(0), "SECOND");
  assert.equal(await reader.read(), true);
  assert.equal(reader.getValue(0), "second set");
  assert.equal(await reader.nextResult(), false);
  await reader.close();
  assert.equal(drained, true);
});

test("JustyBase adapter materializes progressive LOB streams into plain values", async () => {
  const { Db2CompatResult } = require("../dist/cjs/ibmDbCompat.js");
  const { DSS_LIMITS } = require("../dist/cjs/network/dss.js");
  const columns = [
    { name: "B", sqlType: 404, length: 4n, precision: 0, scale: 0, nullable: true },
    { name: "C", sqlType: 408, length: 4n, precision: 0, scale: 0, nullable: true },
    { name: "N", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: true },
  ];
  async function* rows() {
    yield {
      columns,
      values: [
        { kind: "binary-lob", length: 3n, stream: async function* () { yield new Uint8Array([1, 2]); yield new Uint8Array([3]); } },
        { kind: "text-lob", length: 5n, stream: async function* () { yield "ab"; yield "cde"; } },
        null,
      ],
    };
    yield {
      columns,
      values: [
        { kind: "binary-lob", length: 0n, stream: async function* () {} },
        { kind: "text-lob", length: 0n, stream: async function* () {} },
        7,
      ],
    };
  }
  const cursor = new Db2CompatResult(rows(), columns);
  const first = await cursor.fetch();
  assert.ok(Buffer.isBuffer(first.B));
  assert.deepEqual([...first.B], [1, 2, 3]);
  assert.equal(first.C, "abcde");
  assert.equal(first.N, null);
  const second = await cursor.fetch();
  assert.ok(Buffer.isBuffer(second.B));
  assert.equal(second.B.length, 0);
  assert.equal(second.C, "");
  assert.equal(second.N, 7);
  await cursor.close();

  const overLimit = new Db2CompatResult((async function* () {
    yield {
      columns,
      values: [
        { kind: "binary-lob", length: BigInt(DSS_LIMITS.maxLobMaterializationBytes) + 1n, stream: async function* () { throw new Error("stream must not be read"); } },
        "plain",
        null,
      ],
    };
  })(), columns);
  await assert.rejects(() => overLimit.fetch(), /exceeds the supported materialized-value size/);
  await overLimit.close();
});

test("JustyBase adapter class supplies the DatabaseConnection contract shape", async () => {
  const { Db2DatabaseConnection } = require("../dist/cjs/ibmDbCompat.js");
  const connection = new Db2DatabaseConnection({ host: "localhost", database: "SAMPLE", user: "db2inst1" });
  assert.equal(typeof connection.connect, "function");
  assert.equal(typeof connection.close, "function");
  assert.equal(typeof connection.createCommand, "function");
  assert.equal(typeof connection.on, "function");
  assert.equal(typeof connection.removeListener, "function");
  assert.equal(connection._connected, false);
  assert.throws(() => connection.createCommand("VALUES 1"), /connection is not open/i);
});

test("JustyBase adapter validates options before opening any socket", async () => {
  const { Db2DatabaseConnection } = require("../dist/cjs/ibmDbCompat.js");
  const invalidSchema = new Db2DatabaseConnection({
    host: "localhost", database: "SAMPLE", user: "u", options: { currentSchema: "x".repeat(129) },
  });
  await assert.rejects(() => invalidSchema.connect(), /exceeds 128 characters/);
  assert.equal(invalidSchema._connected, false);

  const invalidNul = new Db2DatabaseConnection({
    host: "localhost", database: "SAMPLE", user: "u", options: { currentSchema: "A\u0000B" },
  });
  await assert.rejects(() => invalidNul.connect(), /NUL byte/);
  assert.equal(invalidNul._connected, false);
});

test("JustyBase connection shares one in-flight connect and allows reconnect after close", async () => {
  const { Db2DatabaseConnection, Db2CompatDatabase } = require("../dist/cjs/ibmDbCompat.js");
  const originalOpen = Db2CompatDatabase.open;
  let opens = 0;
  let closed = 0;
  Db2CompatDatabase.open = async () => {
    opens += 1;
    await new Promise((resolve) => setTimeout(resolve, 20));
    return { connection: { addWarningListener: () => () => {} }, close: async () => { closed += 1; } };
  };
  try {
    const connection = new Db2DatabaseConnection({ host: "localhost", database: "SAMPLE", user: "u" });
    await Promise.all([connection.connect(), connection.connect(), connection.connect()]);
    assert.equal(opens, 1);
    assert.equal(connection._connected, true);
    await connection.close();
    assert.equal(closed, 1);
    assert.equal(connection._connected, false);
    await connection.connect();
    assert.equal(opens, 2);
    await connection.close();
    assert.equal(closed, 2);
  } finally {
    Db2CompatDatabase.open = originalOpen;
  }
});

test("JustyBase connection discards a session that resolves after close", async () => {
  const { Db2DatabaseConnection, Db2CompatDatabase } = require("../dist/cjs/ibmDbCompat.js");
  const originalOpen = Db2CompatDatabase.open;
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  let closed = 0;
  Db2CompatDatabase.open = async () => {
    await gate;
    return { connection: { addWarningListener: () => () => {} }, close: async () => { closed += 1; } };
  };
  try {
    const connection = new Db2DatabaseConnection({ host: "localhost", database: "SAMPLE", user: "u" });
    const pending = connection.connect();
    await connection.close();
    release();
    await assert.rejects(() => pending, /closed during connect/);
    assert.equal(closed, 1);
    assert.equal(connection._connected, false);
  } finally {
    Db2CompatDatabase.open = originalOpen;
  }
});

test("compatibility cursor suffixes duplicate and empty labels without losing values", async () => {
  const { Db2CompatResult } = require("../dist/cjs/ibmDbCompat.js");
  const columns = [
    { name: "ID", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: false },
    { name: "ID", sqlType: 449, length: 40n, precision: 40, scale: 0, nullable: true },
    { name: "", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: true },
  ];
  async function* rows() {
    yield { columns, values: [7, "seven", 9] };
  }
  const cursor = new Db2CompatResult(rows(), columns);
  assert.deepEqual(cursor.getColumnNames(), ["ID", "ID__1", "COLUMN3"]);
  assert.deepEqual(cursor.getColumnNamesSync(), ["ID", "ID__1", "COLUMN3"]);
  assert.equal(cursor.getColumnMetadata()[1].SQL_DESC_NAME, "ID", "metadata keeps the server label");
  assert.equal(cursor.getName(1), "ID__1");
  assert.equal(cursor.fieldCount, 3);
  const row = await cursor.fetch();
  assert.deepEqual(row, { ID: 7, ID__1: "seven", COLUMN3: 9 });
  assert.equal(cursor.getValue(0), 7);
  assert.equal(cursor.getValue(1), "seven");
  assert.equal(cursor.getValue(2), 9);
  assert.equal(cursor.getValue(3), undefined);
  await cursor.close();
});

test("compatibility cursor exposes procedure outputs after finish", async () => {
  const { Db2CompatResult } = require("../dist/cjs/ibmDbCompat.js");
  const columns = [{ name: "N", sqlType: 496, length: 4n, precision: 10, scale: 0, nullable: false }];
  const procedure = {
    get columns() { return columns; },
    async *rows() { yield [1]; },
    async nextResultSet() { return false; },
    async finish() { return { rowsAffected: 3, outputParameters: [null, 42] }; },
  };
  async function* noRows() {}
  const reader = new Db2CompatResult(noRows(), columns, procedure);
  assert.throws(() => reader.outputParameters, /only after finish/);
  assert.equal(await reader.read(), true);
  assert.equal(await reader.nextResult(), false);
  assert.deepEqual(reader.outputParameters, [null, 42]);
  const plain = new Db2CompatResult((async function* () {})(), columns);
  assert.throws(() => plain.outputParameters, /no procedure outputs/);
  await reader.close();
  await plain.close();
});

test("compatibility facade exposes SQLCODE/SQLSTATE aliases on failures", async () => {
  const { Db2CompatDatabase } = require("../dist/cjs/ibmDbCompat.js");
  const { Db2ServerError } = require("../dist/cjs/errors.js");
  const failure = new Db2ServerError(-204, "42704", "DB2 SQL error");
  const db = Object.create(Db2CompatDatabase.prototype);
  db.connection = {
    query: async () => { throw failure; },
    execute: async () => { throw failure; },
    prepare: async () => { throw failure; },
    executeProcedureStream: async () => { throw failure; },
  };
  for (const operation of [
    () => db.query("SELECT 1 FROM T"),
    () => db.executeSql("UPDATE T SET ID = 1"),
    () => db.queryResult("SELECT 1 FROM T"),
    () => db.procedureResult("CALL P()"),
  ]) {
    await assert.rejects(operation, (error) => {
      assert.ok(error instanceof Db2ServerError);
      assert.equal(error.sqlCode, -204);
      assert.equal(error.SQLCODE, -204);
      assert.equal(error.SQLSTATE, "42704");
      return true;
    });
  }
});
