'use strict';
require('../load-env.cjs');
const test=require('node:test');
const assert=require('node:assert/strict');
const {Db2Connection}=require('../../dist/cjs/network/session.js');
const { fixtureSchema } = require('./fixture-schema.cjs');
const dsn=process.env.DB2_TEST_DSN;
test('live UR sees dirty data while RS/RR wait for writer rollback', {skip:!dsn}, async()=>{
 const writer=await Db2Connection.connect(dsn), reader=await Db2Connection.connect(dsn);
 const table='ISO_'+require('node:crypto').randomUUID().replaceAll('-','').slice(0,20).toUpperCase();
 let write,read;
 try{
  await writer.execute(`CREATE TABLE ${await fixtureSchema(writer)}.${table} (ID INT NOT NULL PRIMARY KEY, V INT)`);
  await writer.execute(`INSERT INTO ${table} VALUES (1,0)`);
  for(const level of ['UR','RS','RR']){
   write=await writer.beginTransaction();await write.execute(`UPDATE ${table} SET V=99 WHERE ID=1`);
   read=await reader.beginTransaction(level);
   let settled=false;
   const pending=read.query(`SELECT V FROM ${table} WHERE ID=1`,undefined,{timeoutMs:15000});
   pending.then(()=>{settled=true;},()=>{settled=true;});
   if(level==='UR')assert.equal((await pending).rows[0][0],99);
   else {await new Promise(r=>setTimeout(r,200));assert.equal(settled,false);}
   await write.rollback();write=undefined;
   if(level!=='UR')assert.equal((await pending).rows[0][0],0);
   await read.rollback();read=undefined;
   assert.equal((await reader.query(`SELECT V FROM ${table}`)).rows[0][0],0);
  }
 }finally{
  if(write)await write.rollback().catch(()=>{});
  if(read)await read.rollback().catch(()=>{});
  await writer.execute(`DROP TABLE ${await fixtureSchema(writer)}.${table}`).catch(()=>{});
  await reader.close();await writer.close();
 }
});
test('live scoped LUW isolation restores register on commit/rollback', {skip:!dsn}, async()=>{
 const db=await Db2Connection.connect(dsn);
 const iso=async()=> (await db.query('VALUES CURRENT ISOLATION')).rows[0][0].trim();
 try{
  const baseline=await iso();
  for(const level of ['UR','CS','RS','RR']) for(const commit of [false,true]) {
   const tx=await db.beginTransaction(level);
   assert.equal((await tx.query('VALUES CURRENT ISOLATION')).rows[0][0].trim(),level);
   if(commit)await tx.commit();else await tx.rollback();
   assert.equal(await iso(),baseline);
  }
  await db.execute('SET CURRENT ISOLATION UR');
  const tx=await db.beginTransaction('CS');await tx.rollback();
  assert.equal(await iso(),'UR');
  await db.execute('SET CURRENT ISOLATION RESET');
  await assert.rejects(()=>db.beginTransaction('INVALID'),RangeError);
 }finally{await db.close();}
});
