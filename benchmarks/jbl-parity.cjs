require('../tests/load-env.cjs');
const { createHash } = require('node:crypto');
const { Db2Connection } = require('../dist/cjs/network/session.js');

const defaultQueries = [
  ['JBL_CUSTOMERS', 'SELECT * FROM JBL_LIVE.JBL_CUSTOMERS'],
  ['JBL_PRODUCTS', 'SELECT * FROM JBL_LIVE.JBL_PRODUCTS'],
  ['JBL_DEPARTMENTS', 'SELECT * FROM JBL_LIVE.JBL_DEPARTMENTS'],
  ['JBL_EMPLOYEES', 'SELECT * FROM JBL_LIVE.JBL_EMPLOYEES'],
  ['JBL_ORDER_LINES', 'SELECT * FROM JBL_LIVE.JBL_ORDER_LINES'],
  ['JBL_ORDERS', 'SELECT * FROM JBL_LIVE.JBL_ORDERS'],
  ['JBL_PRODUCTS', 'SELECT * FROM JBL_LIVE.JBL_PRODUCTS'],
  ['JBL_SALES_FACT', 'SELECT * FROM JBL_LIVE.JBL_SALES_FACT'],
];

function typeName(sqlType) {
  const base = sqlType % 2 === 1 ? sqlType - 1 : sqlType;
  const types = new Map([
    [384, 'DATE'], [388, 'TIME'], [392, 'TIMESTAMP'],
    [404, 'BLOB'], [408, 'CLOB'], [412, 'DBCLOB'],
    [448, 'VARCHAR'], [452, 'CHAR'], [464, 'VARGRAPHIC'], [468, 'GRAPHIC'],
    [480, 'FLOAT'], [484, 'DECIMAL'], [492, 'BIGINT'], [496, 'INTEGER'],
    [500, 'SMALLINT'], [504, 'DECIMAL'], [908, 'VARBINARY'], [912, 'BINARY'],
    [988, 'XML'], [996, 'DECFLOAT'], [2436, 'BOOLEAN'],
  ]);
  const name = types.get(base);
  if (!name) throw new Error(`Unmapped SQL type ${sqlType} in requested table result`);
  return name;
}

function decimalText(value) {
  const input = String(value).trim();
  if (/^[+-]?(?:infinity|inf|nan|snan)$/i.test(input)) return input.replace(/^\+/, '').toUpperCase();
  const match = /^([+-]?)(\d+)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/.exec(input);
  if (!match) throw new Error(`Invalid decimal result ${input}`);
  let sign = match[1] === '-' ? '-' : '';
  let whole = match[2];
  let fraction = match[3] ?? '';
  const exponent = Number(match[4] ?? '0');
  if (exponent > 0) {
    const move = Math.min(exponent, fraction.length);
    whole += fraction.slice(0, move);
    fraction = fraction.slice(move);
    if (exponent > move) whole += '0'.repeat(exponent - move);
  } else if (exponent < 0) {
    const move = -exponent;
    const combined = whole + fraction;
    const point = whole.length - move;
    if (point <= 0) {
      whole = '0';
      fraction = '0'.repeat(-point) + combined;
    } else {
      whole = combined.slice(0, point);
      fraction = combined.slice(point);
    }
  }
  whole = whole.replace(/^0+(?=\d)/, '');
  fraction = fraction.replace(/0+$/, '');
  if (/^0*$/.test(whole) && fraction.length === 0) sign = '';
  return `${sign}${whole}${fraction.length ? `.${fraction}` : ''}`;
}

function canonicalValue(value, type) {
  if (value === null || value === undefined) return null;
  switch (type) {
    case 'SMALLINT':
    case 'INTEGER':
    case 'BIGINT': return BigInt(value).toString(10);
    case 'DECIMAL':
    case 'DECFLOAT': return decimalText(value);
    case 'FLOAT': {
      const number = Number(value);
      if (!Number.isFinite(number)) return String(number).toUpperCase();
      if (Object.is(number, -0)) return '-0';
      const bits = Buffer.allocUnsafe(8);
      bits.writeDoubleBE(number);
      return bits.toString('hex');
    }
    case 'BOOLEAN':
      if (typeof value !== 'boolean') throw new Error(`Expected BOOLEAN, received ${typeof value}`);
      return value ? 'true' : 'false';
    case 'DATE': {
      if (value instanceof Date) return value.toISOString().slice(0, 10);
      const text = String(value);
      const match = /^(\d{4}-\d{2}-\d{2})/.exec(text);
      if (!match) throw new Error(`Invalid DATE result ${text}`);
      return match[1];
    }
    case 'TIME': {
      const text = value instanceof Date
        ? `${String(value.getUTCHours()).padStart(2, '0')}:${String(value.getUTCMinutes()).padStart(2, '0')}:${String(value.getUTCSeconds()).padStart(2, '0')}.${String(value.getUTCMilliseconds() * 1000).padStart(6, '0')}`
        : String(value);
      const match = /^(\d{2})[.:](\d{2})[.:](\d{2})(?:[.:](\d+))?$/.exec(text);
      if (!match) throw new Error(`Invalid TIME result ${text}`);
      const fraction = (match[4] ?? '').padEnd(6, '0').slice(0, 6);
      return `${match[1]}:${match[2]}:${match[3]}.${fraction}`;
    }
    case 'TIMESTAMP': {
      const text = value instanceof Date
        ? `${value.toISOString().slice(0, 19).replace('T', ' ')}.${String(value.getUTCMilliseconds() * 1000).padStart(6, '0')}`
        : String(value);
      const match = /^(\d{4}-\d{2}-\d{2})[- T](\d{2})[.:](\d{2})[.:](\d{2})(?:[.:](\d+))?$/.exec(text);
      if (!match) throw new Error(`Invalid TIMESTAMP result ${text}`);
      const inputFraction = match[5] ?? '';
      if (inputFraction.length > 12) throw new Error(`TIMESTAMP precision exceeds Db2 maximum: ${text}`);
      const fraction = inputFraction.padEnd(Math.max(6, inputFraction.length), '0');
      return `${match[1]} ${match[2]}:${match[3]}:${match[4]}.${fraction}`;
    }
    case 'CHAR':
    case 'VARCHAR':
    case 'GRAPHIC':
    case 'VARGRAPHIC':
    case 'CLOB':
    case 'DBCLOB':
    case 'XML':
      if (typeof value !== 'string') throw new Error(`Expected ${type} string, received ${typeof value}`);
      return value;
    case 'BINARY':
    case 'VARBINARY':
    case 'BLOB':
      if (!Buffer.isBuffer(value) && !(value instanceof Uint8Array)) throw new Error(`Expected ${type} bytes`);
      return Buffer.from(value).toString('hex');
    default: throw new Error(`Unsupported result type ${type}`);
  }
}

function hashRow(row, columns) {
  if (row.length !== columns.length) throw new Error('Row and column counts differ');
  const hash = createHash('sha256');
  for (let index = 0; index < columns.length; index += 1) {
    const { type } = columns[index];
    const canonical = canonicalValue(row[index], type);
    const typeBytes = Buffer.from(type, 'utf8');
    const valueBytes = canonical === null ? Buffer.alloc(0) : Buffer.from(canonical, 'utf8');
    const typeLength = Buffer.alloc(4);
    typeLength.writeUInt32BE(typeBytes.length);
    const valueLength = Buffer.alloc(4);
    valueLength.writeUInt32BE(valueBytes.length);
    hash.update(Buffer.from([canonical === null ? 1 : 0]));
    hash.update(typeLength);
    hash.update(typeBytes);
    hash.update(valueLength);
    hash.update(valueBytes);
  }
  return hash.digest('hex');
}

function encodeResult(table, columns, rows) {
  const schema = columns.map(({ name, type, scale }) => `${name}:${type}:${scale}`).join(',');
  const hashes = rows.map((row) => hashRow(row, columns)).sort();
  const columnHashes = columns.map((column, index) => rows
    .map((row) => hashRow([row[index]], [column]))
    .sort()
    .join(',')).join('|');
  const captureValues = /\.DB2(?:_PARITY_|PN_|PT_|PX_)/i.test(table);
  const columnValues = captureValues
    ? columns.map((column, index) => rows
      .map((row) => {
        const value = canonicalValue(row[index], column.type);
        return value === null ? 'NULL' : Buffer.from(value, 'utf8').toString('hex');
      })
      .sort()
      .join(',')).join('|')
    : '';
  return `${table}\t${rows.length}\t${schema}\t${hashes.join(',')}\t${columnHashes}\t${columnValues}`;
}

function parseParameterToken(token) {
  if (token === 'N') return null;
  if (token === 'BT') return true;
  if (token === 'BF') return false;
  const separator = token.indexOf(':');
  if (separator !== 1) throw new Error(`Malformed parity parameter token ${token}`);
  const kind = token[0];
  const encoded = token.slice(separator + 1);
  if (kind === 'X') return new Uint8Array(Buffer.from(encoded, 'base64'));
  const value = Buffer.from(encoded, 'base64').toString('utf8');
  switch (kind) {
    case 'I': return BigInt(value);
    case 'D':
    case 'S': return value;
    case 'F': return Number(value);
    default: throw new Error(`Unsupported parity parameter kind ${kind}`);
  }
}

async function main() {
  const dsn = process.env.DB2_TEST_DSN;
  if (!dsn) throw new Error('Set DB2_TEST_DSN');
  const queries = process.env.DB2_PARITY_QUERIES
    ? process.env.DB2_PARITY_QUERIES.split('\n').filter(Boolean).map((line) => {
      const fields = line.split('\t');
      const [table64, sql64, parameterField = ''] = fields;
      if (fields.length !== 3 || !table64 || !sql64) throw new Error('Malformed DB2_PARITY_QUERIES entry');
      const parameters = parameterField === '' ? [] : parameterField.split(',').map(parseParameterToken);
      return {
        table: Buffer.from(table64, 'base64').toString('utf8'),
        sql: Buffer.from(sql64, 'base64').toString('utf8'),
        parameters,
      };
    })
    : defaultQueries.map(([table, sql]) => ({ table, sql, parameters: [] }));
  const db = await Db2Connection.connect(dsn);
  try {
    const records = [];
    for (const { table, sql, parameters } of queries) {
      const result = parameters.length === 0 ? await db.query(sql) : await db.query(sql, parameters);
      const columns = result.columns.map((column) => ({
        name: column.name,
        type: typeName(column.sqlType),
      scale: column.scale,
      }));
      const baseline = encodeResult(table, columns, result.rows);
      const prepared = await db.prepare(sql);
      try {
        const rebound = await prepared.query(parameters);
        const preparedColumns = rebound.columns.map(column => ({ name: column.name, type: typeName(column.sqlType), scale: column.scale }));
        if (encodeResult(table, preparedColumns, rebound.rows) !== baseline) throw new Error(`Prepared read parity failed for ${table}`);
        for (const mode of ['stream', 'prepared-stream']) {
          const rows = [];
          let streamColumns = columns;
          const iterator = mode === 'prepared-stream' || parameters.length > 0 ? prepared.queryStream(parameters) : db.queryStream(sql);
          for await (const row of iterator) {
            streamColumns = row.columns.map(column => ({ name: column.name, type: typeName(column.sqlType), scale: column.scale }));
            const values = [];
            for (const value of row.values) {
              if (value && (value.kind === 'binary-lob' || value.kind === 'text-lob')) {
                if (value.kind === 'text-lob') { let text = ''; for await (const chunk of value.stream()) text += chunk; values.push(text); }
                else { const chunks = []; for await (const chunk of value.stream()) chunks.push(chunk); values.push(Buffer.concat(chunks)); }
              } else values.push(value);
            }
            rows.push(values);
          }
          if (encodeResult(table, streamColumns, rows) !== baseline) throw new Error(`${mode} read parity failed for ${table}`);
        }
      } finally { prepared.close(); }
      records.push(baseline);
    }
    process.stdout.write(`${records.join('\n')}\n`);
  } finally {
    await db.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
