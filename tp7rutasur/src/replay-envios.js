
import { randomUUID } from 'node:crypto';
import { database, kafka, eventSchema, TYPES } from './core.js';

// Colección temporal: NUNCA se modifica "envios".
const DESTINO = 'envios_reconstruidos';

const TOPICS = [
  'envios.estados',
  'envios.promesas',
  'envios.reprogramaciones'
];

const { client, db } = await database();

const admin = kafka.admin();
const consumer = kafka.consumer({
  groupId: `reconstruccion-envios-${randomUUID()}`
});

const coleccion = db.collection(DESTINO);

// Guardamos el offset final de cada partición,
// para saber cuándo terminó el replay.
const limites = new Map();
const avanzados = new Map();

let procesados = 0;
let descartados = 0;

const clave = (topic, partition) =>
  `${topic}:${partition}`;

function terminado() {
  return [...limites.entries()].every(([key, limite]) =>
    (avanzados.get(key) ?? 0) >= limite
  );
}

// Reconstruir los estados del paquete.
async function reconstruirEstado(e) {
  if (!TYPES.includes(e.tipo)) return;

  const anterior = await coleccion.findOne({
    shipmentId: e.shipmentId
  });

  // Evitar duplicados y eventos fuera de orden.
  if (
    anterior?.ultimaSecuencia !== undefined &&
    e.secuencia <= anterior.ultimaSecuencia
  ) {
    return;
  }

  const incremento = e.tipo === 'ENTREGA_FALLIDA'
    ? 1
    : 0;

  await coleccion.updateOne(
    { shipmentId: e.shipmentId },
    {
      $set: {
        estado: e.tipo,
        ultimaSecuencia: e.secuencia,
        hubId: e.hubId,
        zonaId: e.zonaId,
        repartidorId: e.repartidorId
      },
      $inc: {
        intentosFallidos: incremento
      },
      $push: {
        historial: {
          eventId: e.eventId,
          tipo: e.tipo,
          ocurridoEn: e.ocurridoEn,
          motivo: e.motivo || null,
          intento: e.intento || null
        }
      }
    },
    { upsert: true }
  );
}

// Reconstruir promesas de entrega.
async function reconstruirPromesa(e) {
  await coleccion.updateOne(
    { shipmentId: e.shipmentId },
    {
      $set: {
        promesa: {
          venceEn: e.venceEn
        }
      }
    },
    { upsert: true }
  );
}

// Reconstruir decisiones de reprogramación.
async function reconstruirReprogramacion(e) {
  await coleccion.updateOne(
    { shipmentId: e.shipmentId },
    {
      $set: {
        reprogramacion: {
          accion: e.accion,
          fecha: e.fecha || null,
          franja: e.franja || null
        }
      }
    },
    { upsert: true }
  );
}

let completar;
let fallar;

const finalizacion = new Promise((resolve, reject) => {
  completar = resolve;
  fallar = reject;
});

async function main() {
  console.log('====================================');
  console.log('RECONSTRUCCION DE ENVIOS DESDE KAFKA');
  console.log('====================================');

  // Solo limpiamos la colección de PRUEBA.
  await coleccion.deleteMany({});

  await coleccion.createIndex(
    { shipmentId: 1 },
    { unique: true }
  );

  await admin.connect();

  // Fotografiar el final de cada topic.
  for (const topic of TOPICS) {
    const offsets = await admin.fetchTopicOffsets(topic);

    for (const item of offsets) {
      const key = clave(topic, item.partition);
      const limite = Number(item.offset);

      limites.set(key, limite);
      avanzados.set(key, 0);
    }
  }

  console.log('Offsets finales registrados.');

  await consumer.connect();

  for (const topic of TOPICS) {
    await consumer.subscribe({
      topic,
      fromBeginning: true
    });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const key = clave(topic, partition);
        const offset = Number(message.offset);

        // No incorporar eventos posteriores al punto
        // de referencia de esta reconstrucción.
        if (offset < limites.get(key)) {
          let e;

          try {
            e = JSON.parse(message.value.toString());
          } catch {
            descartados++;
            e = null;
          }

          if (e) {
            const resultado = eventSchema.safeParse(e);

            if (!resultado.success) {
              descartados++;
            } else {
              if (topic === 'envios.estados') {
                await reconstruirEstado(e);
              } else if (topic === 'envios.promesas') {
                await reconstruirPromesa(e);
              } else if (topic === 'envios.reprogramaciones') {
                await reconstruirReprogramacion(e);
              }

              procesados++;
            }
          }
        }

        avanzados.set(
          key,
          Math.max(avanzados.get(key) ?? 0, offset + 1)
        );

        if (procesados > 0 && procesados % 500 === 0) {
          console.log(`Eventos leídos: ${procesados}`);
        }

        if (terminado()) {
          completar();
        }
      } catch (error) {
        fallar(error);
        throw error;
      }
    }
  });

  if (terminado()) {
    completar();
  }

  await finalizacion;

  console.log('');
  console.log('RECONSTRUCCION FINALIZADA');
  console.log(`Eventos válidos leídos: ${procesados}`);
  console.log(`Eventos inválidos: ${descartados}`);
  console.log(
    `Envíos reconstruidos: ${await coleccion.countDocuments({})}`
  );
}

try {
  await main();
} catch (error) {
  console.error('ERROR EN RECONSTRUCCION:', error);
  process.exitCode = 1;
} finally {
  await Promise.allSettled([
    consumer.disconnect(),
    admin.disconnect(),
    client.close()
  ]);
}
