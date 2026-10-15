const test = require("node:test");
const assert = require("node:assert/strict");
const { Db2CompatDatabase, Db2CompatResult } = require("../dist/cjs/ibmDbCompat.js");
const { Db2ServerError } = require("../dist/cjs/errors.js");

// Column shapes follow the ColumnDescription fields the driver decodes from DRDA metadata.
const column = (name, sqlType, length, precision = 0, scale = 0) => ({
  name, sqlType, length: BigInt(length), precision, scale, nullable: false,
});

test("column metadata matches node-ibm_db (Db2 LUW 12.1.5 probe), except SQL_DESC_PRECISION", async () => {
  const columns = [
    column("SI", 500, 2, 5),
    column("I", 496, 4, 10),
    column("BG", 492, 8, 19),
    column("DB", 480, 8, 53),
    column("RE", 480, 4, 24),
    column("D52", 484, 0, 5, 2),
    column("D150", 484, 0, 15, 0),
    column("CH", 452, 5),
    column("VC", 448, 20),
    column("DT", 384, 10),
    column("TM", 388, 8),
    column("TS", 392, 26, 26, 6),
    column("TS0", 392, 19, 19, 0),
    column("B", 404, 10),
    column("C", 408, 20),
    column("BN", 912, 2),
  ];
  const cursor = new Db2CompatResult((async function* () {})(), columns);
  const expected = [
    ["SI", "SMALLINT", 5, 6, 6, 5, 0],
    ["I", "INTEGER", 4, 11, 11, 10, 0],
    ["BG", "BIGINT", -5, 20, 20, 19, 0],
    ["DB", "DOUBLE", 8, 22, 22, 53, 0],
    ["RE", "REAL", 7, 13, 13, 24, 0],
    ["D52", "DECIMAL", 3, 7, 7, 5, 2],
    ["D150", "DECIMAL", 3, 17, 17, 15, 0],
    ["CH", "CHAR", 1, 5, 5, 0, 0],
    ["VC", "VARCHAR", 12, 20, 20, 0, 0],
    ["DT", "DATE", 91, 10, 10, 0, 0],
    ["TM", "TIME", 92, 8, 8, 0, 0],
    ["TS", "TIMESTAMP", 93, 26, 26, 26, 6],
    ["TS0", "TIMESTAMP", 93, 19, 19, 19, 0],
    ["B", "BLOB", -98, 20, 10, 0, 0],
    ["C", "CLOB", -99, 20, 20, 0, 0],
    ["BN", "BINARY", -2, 4, 2, 0, 0],
  ];

  const metadata = cursor.getColumnMetadata();
  assert.deepEqual(metadata.map((item) => item.index), expected.map((_, index) => index + 1));
  assert.deepEqual(
    metadata.map((item) => [
      item.SQL_DESC_NAME,
      item.SQL_DESC_TYPE_NAME,
      item.SQL_DESC_CONSIZE_TYPE,
      item.SQL_DESC_DISPLAY_SIZE,
      item.SQL_DESC_LENGTH,
      item.SQL_DESC_PRECISION,
      item.SQL_DESC_SCALE,
    ]),
    expected,
  );
});

test("a 4-byte FLOAT column is REAL, an 8-byte one is DOUBLE", () => {
  const cursor = new Db2CompatResult((async function* () {})(), [column("A", 480, 4), column("B", 480, 8)]);
  assert.deepEqual(cursor.getColumnMetadata().map((item) => item.SQL_DESC_TYPE_NAME), ["REAL", "DOUBLE"]);
});

test("inline BLOB values are Buffers from query() and from the fetch cursor, as in node-ibm_db", async () => {
  const columns = [column("B", 404, 3)];
  const bytes = new Uint8Array([1, 2, 255]);
  const db = Object.create(Db2CompatDatabase.prototype);
  db.connection = { query: async () => ({ columns, rows: [[bytes]] }) };

  const [row] = await db.query("SELECT B FROM T");
  assert.equal(Buffer.isBuffer(row.B), true);
  assert.deepEqual([...row.B], [1, 2, 255]);

  async function* rows() { yield { columns, values: [bytes] }; }
  const cursor = new Db2CompatResult(rows(), columns);
  const fetched = await cursor.fetch();
  assert.equal(Buffer.isBuffer(fetched.B), true);
  await cursor.close();
});

test("a server error message carries the node-ibm_db SQLnnnnN token and both SQLSTATE spellings", async () => {
  const serverError = new Db2ServerError(-204, "42704", "DB2INST1.MISSING is an undefined name.");
  const db = Object.create(Db2CompatDatabase.prototype);
  db.connection = { query: async () => { throw serverError; } };

  await assert.rejects(db.query("SELECT * FROM MISSING"), (error) => {
    assert.equal(error.message, "[JustyBase][DB2] SQL0204N  DB2INST1.MISSING is an undefined name.  SQLSTATE=42704");
    assert.equal(error.sqlcode, -204);
    assert.equal(error.sqlstate, "42704");
    assert.equal(error.SQLCODE, -204);
    assert.equal(error.SQLSTATE, "42704");
    // The native fields keep the driver's values.
    assert.equal(error.sqlCode, -204);
    assert.equal(error.sqlState, "42704");
    assert.match(error.stack, /SQL0204N/);
    return true;
  });
});

test("a fetch failure uses the same message shape as a query failure", async () => {
  const columns = [column("ID", 496, 4)];
  async function* rows() {
    yield { columns, values: [1] };
    throw new Db2ServerError(-952, "57014", "Query cancelled by interrupt.");
  }
  const cursor = new Db2CompatResult(rows(), columns);
  assert.deepEqual(await cursor.fetch(), { ID: 1 });

  await assert.rejects(cursor.fetch(), (error) => {
    assert.equal(error.message, "[JustyBase][DB2] SQL0952N  Query cancelled by interrupt.  SQLSTATE=57014");
    assert.equal(error.sqlcode, -952);
    return true;
  });
});

test("errors without a server SQLCODE keep their message", async () => {
  const plain = new Error("connection reset");
  const db = Object.create(Db2CompatDatabase.prototype);
  db.connection = { query: async () => { throw plain; } };

  await assert.rejects(db.query("VALUES 1"), (error) => {
    assert.equal(error.message, "connection reset");
    assert.equal(error.sqlcode, undefined);
    return true;
  });
});

test("decimal and bigint value options are opt-in and default to exact values", async () => {
  const columns = [column("D", 484, 0, 5, 2), column("BG", 492, 8, 19), column("S", 448, 5)];
  const connection = { query: async () => ({ columns, rows: [["1.25", 9007199254740993n, "x"]] }) };
  const make = (valueOptions) => {
    const db = Object.create(Db2CompatDatabase.prototype);
    db.connection = connection;
    db.valueOptions = valueOptions;
    return db;
  };

  assert.deepEqual(await make(undefined).query("SELECT"), [{ D: "1.25", BG: 9007199254740993n, S: "x" }]);
  assert.deepEqual(
    await make({ decimal: "number", bigint: "string" }).query("SELECT"),
    [{ D: 1.25, BG: "9007199254740993", S: "x" }],
  );
});
