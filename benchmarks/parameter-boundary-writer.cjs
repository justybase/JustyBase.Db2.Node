'use strict';
require('../tests/load-env.cjs');
const {Db2Connection}=require('../dist/cjs/network/session.js');
(async()=>{
 const [table, mode, rawRows]=process.argv.slice(2);
 const rows=Number(rawRows);
 if(!/^BP_[A-F0-9]{32}$/.test(table)||!['direct','prepared'].includes(mode)||![21,22,37,85,275,1000].includes(rows)) throw new Error('Invalid controlled fixture arguments');
 const db=await Db2Connection.connect(process.env.DB2_TEST_DSN);
 try {
  for(const width of [85,169]) {
   const wide=await db.query('SELECT '+Array.from({length:width},(_,i)=>`${i+1} AS C${i+1}`).join(',')+' FROM SYSIBM.SYSDUMMY1');
   if(wide.columns.length!==width||wide.rows.length!==1||wide.rows[0].length!==width||wide.rows[0].some((v,i)=>v!==i+1)) throw new Error('Wide result descriptor/value mismatch');
  }
  const sql=`INSERT INTO ${table} VALUES `+Array(rows).fill('(?,?,?,?)').join(',');
  const values=Array.from({length:rows},(_,j)=>{const i=j+1;return [i,i*101,i%2===0?null:-i,i*10007];}).flat();
  if(mode==='prepared'){const s=await db.prepare(sql);try{await s.execute(values);}finally{s.close();}}
  else await db.execute(sql,values);
 }finally{await db.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
