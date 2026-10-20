// Endpoint de control solo para entorno local; no expone Kafka al navegador.
export async function POST(request){if(process.env.NODE_ENV!=='production'&&process.env.DEMO_TOKEN===undefined)return Response.json({error:'Token no configurado'},{status:503});
 try{const {scenario}=await request.json();if(!['feliz','ausente','demorado','tercer-fallo','rafaga','invalido'].includes(scenario))return Response.json({error:'Escenario inválido'},{status:400});
 const r=await fetch(`${process.env.SIMULATOR_URL}/scenario/${scenario}`,{method:'POST',headers:{'x-demo-token':process.env.DEMO_TOKEN},signal:AbortSignal.timeout(120000)});if(!r.ok)throw Error('Simulador no disponible');return Response.json(await r.json())}catch{return Response.json({error:'No se pudo ejecutar el escenario'},{status:500})}}
