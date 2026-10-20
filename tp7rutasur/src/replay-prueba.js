import {database,kafka} from './core.js';
const {db}=await database();const consumer = kafka.consumer({
  groupId: 'analitica-hub-prueba-' + Date.now()
});
await consumer.connect();await consumer.subscribe({topic:'envios.estados',fromBeginning:true});
console.log('Replay histórico iniciado; Ctrl+C para terminar');
await consumer.run({eachMessage:async({message})=>{
 let e;try{e=JSON.parse(message.value.toString())}catch{return}
 if(!['INGRESADO_HUB','EN_REPARTO'].includes(e.tipo))return;
 const old=await db.collection('analitica_tiempos_prueba').findOne({shipmentId:e.shipmentId})||{};
 if(e.tipo==='INGRESADO_HUB')await db.collection('analitica_tiempos_prueba').updateOne({shipmentId:e.shipmentId},{$set:{hubId:e.hubId,ingreso:e.ocurridoEn}},{upsert:true});
 else if(old.ingreso&&!old.salida){const duracion=(new Date(e.ocurridoEn)-new Date(old.ingreso))/60000;await db.collection('analitica_tiempos_prueba').updateOne({shipmentId:e.shipmentId},{$set:{salida:e.ocurridoEn,minutos:duracion}})}
}});
