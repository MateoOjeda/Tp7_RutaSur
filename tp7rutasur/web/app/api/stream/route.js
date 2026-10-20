import {db} from '../../lib';
export const runtime='nodejs';export const dynamic='force-dynamic';
export async function GET(request){const encoder=new TextEncoder();let streams=[];const output=new ReadableStream({async start(controller){
 const send=msg=>{try{controller.enqueue(encoder.encode(`data: ${JSON.stringify(msg)}\n\n`))}catch{}};
 try{const d=await db();streams=await Promise.all(['envios','alertas','notificaciones'].map(async name=>{const cs=d.collection(name).watch([],{fullDocument:'updateLookup'});cs.on('change',()=>send({changed:true}));cs.on('error',()=>send({error:'Reconectando'}));return cs}));send({ready:true})}catch{send({error:'Change streams no disponibles'})}
 request.signal.addEventListener('abort',()=>{for(const cs of streams)cs.close().catch(()=>{});try{controller.close()}catch{}},{once:true})
 },cancel(){for(const cs of streams)cs.close().catch(()=>{})}});return new Response(output,{headers:{'Content-Type':'text/event-stream','Cache-Control':'no-cache, no-transform','Connection':'keep-alive'}})}
