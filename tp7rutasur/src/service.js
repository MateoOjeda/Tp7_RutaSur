import {randomUUID} from 'node:crypto';
import {database,producer,publish,mkEvent,consume,processed,TYPES} from './core.js';
const role=process.env.ROLE;if(!role)throw Error('ROLE es obligatorio');
const {client,db}=await database(),p=producer();await p.connect();
const coll=n=>db.collection(n);
// Las transacciones protegen la deduplicación y la escritura de proyecciones.
async function once(group,e,fn){const s=client.startSession();try{await s.withTransaction(async()=>{if(await processed(db,group,e.eventId,s))return;await fn(s)})}finally{await s.endSession()}}
async function project(e,topic){
 await once('proyector',e,async(session)=>{
 if(topic==='envios.promesas'){
  await coll('envios').updateOne({shipmentId:e.shipmentId},{$set:{promesa:{venceEn:e.venceEn}}},{upsert:true,session});
 }else if(topic==='envios.estados'){
  if(!TYPES.includes(e.tipo))throw new Error('Tipo de estado desconocido');
  const old=await coll('envios').findOne({shipmentId:e.shipmentId},{session});
  if(old&&e.secuencia<=old.ultimaSecuencia)return;
  const inc=e.tipo==='ENTREGA_FALLIDA'?1:0;
  await coll('envios').updateOne({shipmentId:e.shipmentId},{$set:{estado:e.tipo,ultimaSecuencia:e.secuencia,hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId},$inc:{intentosFallidos:inc},$push:{historial:{eventId:e.eventId,tipo:e.tipo,ocurridoEn:e.ocurridoEn,motivo:e.motivo||null,intento:e.intento||null}}},{upsert:true,session});
 }else if(topic==='envios.reprogramaciones'){
  await coll('envios').updateOne({shipmentId:e.shipmentId},{$set:{reprogramacion:{accion:e.accion,fecha:e.fecha||null,franja:e.franja||null}}},{upsert:true,session});
 }else if(topic==='notificaciones.enviadas'){
  await coll('notificaciones').updateOne({eventId:e.eventId},{$setOnInsert:{...e}},{upsert:true,session});
 }else if(topic==='alertas.sla'){
  const estado=e.accion==='CIERRE'?'CERRADA':'ABIERTA';
  await coll('alertas').updateOne({shipmentId:e.shipmentId,regla:e.regla},{$set:{estado,prioridad:e.prioridad,causa:e.regla,ocurridoEn:e.ocurridoEn},$setOnInsert:{shipmentId:e.shipmentId,regla:e.regla}},{upsert:true,session});
 }
 });
}
async function notification(e,topic){
 if(topic==='envios.estados'&&e.tipo!=='ENTREGA_FALLIDA')return;
 if(topic==='alertas.sla'&&e.accion==='CIERRE')return;
 // Marca persistente antes de publicar: evita efectos duplicados; un outbox sería más robusto ante caídas entre ambas acciones.
 const key=topic+':'+e.eventId;
 const r=await coll('salidas_pendientes').updateOne({_id:'notify:'+key},{$setOnInsert:{_id:'notify:'+key,created:new Date(),published:false}},{upsert:true});
 if(!r.upsertedCount)return;
 const out=mkEvent(e.shipmentId,'NOTIFICACION',e.secuencia,{hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId,origen:e.eventId,mensaje:e.tipo==='ENTREGA_FALLIDA'?'Entrega fallida. Opciones de reprogramación':'Alerta SLA'},new Date(e.ocurridoEn));
 await publish(p,'notificaciones.enviadas',out);await coll('salidas_pendientes').updateOne({_id:'notify:'+key},{$set:{published:true}});
}
function nextWorkday(str){const d=new Date(str);d.setUTCDate(d.getUTCDate()+1);while([0,6].includes(d.getUTCDay()))d.setUTCDate(d.getUTCDate()+1);return d.toISOString()}
async function reprogram(e){if(e.tipo!=='ENTREGA_FALLIDA')return;
 const key='retry:'+e.eventId;const r=await coll('salidas_pendientes').updateOne({_id:key},{$setOnInsert:{created:new Date(),published:false}},{upsert:true});if(!r.upsertedCount)return;
 const n=e.intento||1;let accion='RETENER',fecha=null,franja=null;
 if(n>=3||e.motivo==='RECHAZADO'||e.motivo==='ZONA_INACCESIBLE')accion='DEVOLVER';
 else if(e.motivo==='AUSENTE'){accion='REPROGRAMAR';fecha=nextWorkday(e.ocurridoEn);franja=e.franja==='TARDE'?'MAÑANA':'TARDE'}
 const out=mkEvent(e.shipmentId,'REPROGRAMACION',e.secuencia,{hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId,accion,fecha,franja,origen:e.eventId},new Date(e.ocurridoEn));await publish(p,'envios.reprogramaciones',out);await coll('salidas_pendientes').updateOne({_id:key},{$set:{published:true}});
}
function bucket(e){return {fecha:e.ocurridoEn.slice(0,10),zonaId:e.zonaId,hubId:e.hubId,franja:e.franja|| (new Date(e.ocurridoEn).getUTCHours()<14?'MAÑANA':'TARDE')}}
async function analytics(e){
 if(e.tipo!=='ENTREGADO'&&e.tipo!=='ENTREGA_FALLIDA')return;
 await once('analitica',e,async(session)=>{const b=bucket(e),fail=e.tipo==='ENTREGA_FALLIDA';
 // Un intento exitoso se cuenta como primer intento únicamente si intento === 1.
 const inc={intentos:1,fallos:fail?1:0,entregasPrimerIntento:!fail&&(e.intento||1)===1?1:0,entregados:fail?0:1,fallosSLA:!fail&&e.venceEn&&new Date(e.ocurridoEn)>new Date(e.venceEn)?1:0};if(fail)inc['motivos.'+e.motivo]=1;
 await coll('metricas_diarias').updateOne(b,{$inc:inc,$setOnInsert:b},{upsert:true,session});});
}
// Monitor: conserva estado/alertas en MongoDB, emite cambios por Kafka, lee el reloj simulado.
const factor=Number(process.env.SIM_FACTOR||120);let clockAnchor=null;
function virtualNow(){return clockAnchor?new Date(clockAnchor.virtual+(Date.now()-clockAnchor.real)*factor):new Date()}
async function alert(e,regla,prioridad,accion='APERTURA'){
 const key=`${e.shipmentId}:${regla}`;const set={shipmentId:e.shipmentId,regla,prioridad,accion,created:new Date(),published:false};
 const r=await coll('sla_emisiones').updateOne({_id:key+':'+accion},{$setOnInsert:set},{upsert:true});if(!r.upsertedCount)return;
 const out=mkEvent(e.shipmentId,'ALERTA_SLA',e.secuencia,{hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId,regla,prioridad,accion},virtualNow());await publish(p,'alertas.sla',out);await coll('sla_emisiones').updateOne({_id:key+':'+accion},{$set:{published:true}});
}
async function checkSla(x){if(!x||x.estado==='ENTREGADO'||!x.venceEn)return;
 const hours=(new Date(x.venceEn)-virtualNow())/3600000;
 if(hours<0)await alert(x,'VENCIDO','CRITICA');
 else if(x.estado==='ENTREGA_FALLIDA'&&new Date(x.venceEn).toISOString().slice(0,10)===virtualNow().toISOString().slice(0,10))await alert(x,'FALLO_DIA_LIMITE','ALTA');
 else if(hours<4&&!['EN_REPARTO','ENTREGA_FALLIDA'].includes(x.estado))await alert(x,'SIN_SALIDA','ALTA');
 else if(hours<10&&x.estado==='CREADO')await alert(x,'SIN_INGRESO','MEDIA');
}
async function monitor(e,topic){
 clockAnchor={real:Date.now(),virtual:new Date(e.ocurridoEn).getTime()};
 const old=await coll('sla_estado').findOne({shipmentId:e.shipmentId})||{};
 const patch={shipmentId:e.shipmentId,hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId,secuencia:e.secuencia};
 if(topic==='envios.promesas')patch.venceEn=e.venceEn;
 else if(!old.estado||e.secuencia>=(old.secuencia??-1))patch.estado=e.tipo;
 await coll('sla_estado').updateOne({shipmentId:e.shipmentId},{$set:patch},{upsert:true});
 const x=await coll('sla_estado').findOne({shipmentId:e.shipmentId});
 if(x.estado==='ENTREGADO'){for(const a of await coll('sla_emisiones').find({shipmentId:e.shipmentId,accion:'APERTURA'}).toArray())await alert(x,a.regla,a.prioridad,'CIERRE')}
 else await checkSla(x);
}
const handlers={proyector:{topics:['envios.estados','envios.promesas','envios.reprogramaciones','alertas.sla','notificaciones.enviadas'],fn:project},notificaciones:{topics:['envios.estados','alertas.sla'],fn:notification},reprogramacion:{topics:['envios.estados'],fn:reprogram},'monitor-sla':{topics:['envios.estados','envios.promesas'],fn:monitor},analitica:{topics:['envios.estados'],fn:analytics}};
if(!handlers[role])throw Error('ROLE desconocido: '+role);
if(role==='monitor-sla')setInterval(async()=>{try{for(const x of await coll('sla_estado').find({estado:{$ne:'ENTREGADO'}}).toArray())await checkSla(x)}catch(e){console.error(e)}},60000);
await consume({groupId:role,topics:handlers[role].topics,onEvent:handlers[role].fn,db});
