'use strict';
async function fixtureSchema(connection) {
  const schema = String((await connection.query('VALUES CURRENT SCHEMA')).rows[0][0]).trimEnd();
  return '"' + schema.replaceAll('"', '""') + '"';
}
module.exports = { fixtureSchema };
