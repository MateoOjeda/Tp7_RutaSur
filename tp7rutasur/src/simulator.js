import {createServer} from 'node:http';
import {randomUUID} from 'node:crypto';
import {producer,publish,mkEvent} from './core.js';
const p=producer();await p.connect();
let rng=Number(process.env.SIM_SEED||12345);const rand=()=>((rng=(Math.imul(rng,1664525)+1013904223)>>>0)/4294967296);
const factor=Number(process.env.SIM_FACTOR||120),startReal=Date.now(),startVirtual=Date.UTC(2026,9,20,8);
const now=()=>new Date(startVirtual+(Date.now()-startReal)*factor);
const emit=async(topic,e)=>{await publish(p,topic,e);console.log('PUBLICADO',topic,e.tipo,e.shipmentId)};
let num=0;
async function shipment(scenario='aleatorio'){
  const id=`RS-${String(++num).padStart(6,'0')}-${randomUUID().slice(0,6)}`;
  const zona=['Z1','Z2','Z3'][Math.floor(rand()*3)],hub=['H1','H2','H3'][Math.floor(rand()*3)],repartidor='R'+Math.ceil(rand()*120);
  const base={hubId:hub,zonaId:zona,repartidorId:repartidor};let seq=0;
  const state=async(tipo,extra={})=>{const e=mkEvent(id,tipo,++seq,{...base,...extra},now());await emit('envios.estados',e);return e};
  const promise=mkEvent(id,'PROMESA',0,{...base,venceEn:new Date(now().getTime()+8*3600000).toISOString(),relojFactor:factor},now());await emit('envios.promesas',promise);
  const created=await state('CREADO');await state('INGRESADO_HUB');
  if(scenario==='demorado'){await emit('envios.promesas',{...promise,eventId:randomUUID(),venceEn:new Date(now().getTime()+2*3600000).toISOString()});return id}
  await state('CLASIFICADO');await state('EN_REPARTO');
  const failure=scenario==='ausente'||scenario==='tercer-fallo'||(scenario==='aleatorio'&&rand()<(zona==='Z3'?.3:zona==='Z2'?.15:.05));
  if(failure){const motivos=['AUSENTE','DIRECCION_INCORRECTA','RECHAZADO','ZONA_INACCESIBLE'];const r=rand();const motivo=scenario!=='aleatorio'?'AUSENTE':motivos[r<.6?0:r<.8?1:r<.95?2:3];
    if(scenario==='tercer-fallo'){for(let i=1;i<=3;i++){if(i>1)await state('EN_REPARTO',{intento:i});await state('ENTREGA_FALLIDA',{motivo,intento:i,franja:i%2?'MAÑANA':'TARDE'})}}
    else await state('ENTREGA_FALLIDA',{motivo,intento:1,franja:'MAÑANA'});
  }else await state('ENTREGADO',{intento:1});
  if(rand()<.02)await emit('envios.estados',created);
  if(rand()<.02)await emit('envios.estados',mkEvent(id,'CLASIFICADO',2,base,now()));
  if(rand()<.01)await emit('envios.estados',{...mkEvent(id,'ESTADO_INVALIDO',++seq,base,now()),secuencia:'mal'});
  return id;
}
const handlers={feliz:()=>shipment('feliz'),ausente:()=>shipment('ausente'),demorado:()=>shipment('demorado'),'tercer-fallo':()=>shipment('tercer-fallo'),rafaga:async()=>{const ids=[];for(let i=0;i<500;i++)ids.push(await shipment());return `${ids.length} envíos`},invalido:async()=>{const e=mkEvent('RS-INVALIDO','CREADO',1,{},now());e.secuencia='invalida';await emit('envios.estados',e);return e.shipmentId}};
const arg=process.argv[2];if(arg){if(!handlers[arg])throw Error('Escenario desconocido');console.log(await handlers[arg]());await p.disconnect();process.exit(0)}
// Endpoint exclusivamente local de control para la demo (no transporta eventos de negocio entre servicios).
createServer(async(req,res)=>{
 if(req.method!=='POST'||!req.url?.startsWith('/scenario/')){res.writeHead(404).end();return}
 if(req.headers['x-demo-token']!==process.env.DEMO_TOKEN){res.writeHead(403).end('Prohibido');return}
 const name=decodeURIComponent(req.url.slice('/scenario/'.length));if(!handlers[name]){res.writeHead(400).end('Escenario invalido');return}
 try{const id=await handlers[name]();res.writeHead(200,{'Content-Type':'application/json'}).end(JSON.stringify({shipmentId:id}))}
 catch(e){console.error(e);res.writeHead(500).end('Error del escenario')}
}).listen(3001,'0.0.0.0');
// 20 envíos por minuto si SIM_INTERVAL_MS=3000.
setInterval(()=>shipment().catch(console.error),Number(process.env.SIM_INTERVAL_MS||3000));
console.log('Simulador iniciado');
