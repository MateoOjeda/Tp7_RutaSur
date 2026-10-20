
import { Kafka, logLevel } from 'kafkajs';
import { MongoClient } from 'mongodb';
import { z } from 'zod';
import { randomUUID } from 'node:crypto';

// 1. Configuración de Kafka
export const kafka = new Kafka({
  clientId: `rutasur-${process.env.ROLE || 'sim'}`,
  brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
  logLevel: logLevel.WARN
});

// 2. Estados permitidos para un envío
export const TYPES = [
  'CREADO',
  'INGRESADO_HUB',
  'CLASIFICADO',
  'EN_REPARTO',
  'ENTREGADO',
  'ENTREGA_FALLIDA'
];

// 3. Esquema de validación de eventos
export const eventSchema = z.object({
  eventId: z.string().uuid(),
  shipmentId: z.string().min(1).max(100),
  tipo: z.string().min(1),
  ocurridoEn: z.string().datetime(),
  secuencia: z.number().int().nonnegative(),
  hubId: z.string(),
  zonaId: z.string(),
  repartidorId: z.string(),
  version: z.literal(1)
}).passthrough();

// 4. Crear un nuevo evento
export const mkEvent = (
  shipmentId,
  tipo,
  secuencia,
  extra = {},
  clock = new Date()
) => ({
  eventId: randomUUID(),
  shipmentId,
  tipo,
  ocurridoEn: clock.toISOString(),
  secuencia,
  hubId: 'H1',
  zonaId: 'Z1',
  repartidorId: 'R1',
  version: 1,
  ...extra
});

// 5. Crear un productor de Kafka
export const producer = () => kafka.producer({
  allowAutoTopicCreation: false
});

// 6. Publicar un evento
export async function publish(p, topic, event) {
  await p.send({
    topic,
    messages: [{
      key: event.shipmentId,
      value: JSON.stringify(event)
    }]
  });
}

// 7. Conexión y configuración de MongoDB
export async function database() {
  const client = new MongoClient(
    process.env.MONGO_URI ||
    'mongodb://mongo:27017/rutasur?replicaSet=rs0'
  );

  await client.connect();

  const db = client.db('rutasur');

  await Promise.all([
    db.collection('envios').createIndex(
      { shipmentId: 1 }, { unique: true }
    ),
    db.collection('envios').createIndex(
      { estado: 1, zonaId: 1 }
    ),
    db.collection('envios').createIndex(
      { 'promesa.venceEn': 1 }
    ),
    db.collection('alertas').createIndex(
      { estado: 1, prioridad: 1 }
    ),
    db.collection('alertas').createIndex(
      { shipmentId: 1 }
    ),
    db.collection('alertas').createIndex(
      { shipmentId: 1, regla: 1 }, { unique: true }
    ),
    db.collection('notificaciones').createIndex(
      { shipmentId: 1 }
    ),
    db.collection('notificaciones').createIndex(
      { eventId: 1 }, { unique: true }
    ),
    db.collection('metricas_diarias').createIndex(
      { fecha: 1, zonaId: 1, franja: 1, hubId: 1 },
      { unique: true }
    ),
    db.collection('eventos_procesados').createIndex(
      { consumidor: 1, eventId: 1 }, { unique: true }
    ),
    db.collection('eventos_procesados').createIndex(
      { creadoEn: 1 }, { expireAfterSeconds: 604800 }
    ),
    db.collection('sla_estado').createIndex(
      { shipmentId: 1 }, { unique: true }
    ),
    db.collection('analitica_tiempos').createIndex(
      { shipmentId: 1 }, { unique: true }
    )
  ]);

  return { client, db };
}

// 8. Convertir un error en un mensaje corto
export function failReason(err) {
  return String(err?.message || err).slice(0, 300);
}

// 9. Comprobar eventos procesados
// CAMBIO PRINCIPAL: consultar antes de intentar insertar.
export async function processed(db, consumidor, eventId, session) {
  const collection = db.collection('eventos_procesados');

  const existing = await collection.findOne(
    { consumidor, eventId },
    { session }
  );

  if (existing) {
    // Ya se procesó: no volver a aplicar sus efectos.
    return true;
  }

  // Registramos el evento dentro de la transacción.
  // Si otra transacción produce una colisión, el error
  // debe salir para que se pueda reintentar la operación.
  await collection.insertOne(
    {
      consumidor,
      eventId,
      creadoEn: new Date()
    },
    { session }
  );

  return false;
}

// 10. Consumir eventos de Kafka
export async function consume({
  groupId,
  topics,
  onEvent,
  db,
  sessionOnEvent
}) {
  const c = kafka.consumer({
    groupId,
    allowAutoTopicCreation: false
  });

  const dlq = producer();

  await dlq.connect();
  await c.connect();

  for (const topic of topics) {
    await c.subscribe({
      topic,
      fromBeginning: true
    });
  }

  await c.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {
      const offset = message.offset;
      let event;

      try {
        // Convertir el mensaje JSON en un objeto.
        event = JSON.parse(message.value.toString());

        // Validar los campos obligatorios.
        eventSchema.parse(event);

        // Validar los estados conocidos.
        if (
          topic === 'envios.estados' &&
          !TYPES.includes(event.tipo)
        ) {
          throw new z.ZodError([{
            code: 'custom',
            path: ['tipo'],
            message: 'Estado inválido'
          }]);
        }

        // Ejecutar la función del servicio consumidor.
        await onEvent(event, topic, {
          partition,
          offset,
          db,
          sessionOnEvent
        });

      } catch (err) {
        // Los eventos inválidos se envían a la DLQ.
        if (
          err instanceof SyntaxError ||
          err instanceof z.ZodError
        ) {
          const bad = {
            eventId: randomUUID(),
            shipmentId:
              event?.shipmentId ||
              message.key?.toString() ||
              'desconocido',
            tipo: 'DLQ',
            ocurridoEn: new Date().toISOString(),
            secuencia: 0,
            hubId: '',
            zonaId: '',
            repartidorId: '',
            version: 1,
            origen: topic,
            razon: failReason(err),
            payload: message.value.toString()
          };

          await publish(dlq, 'envios.estados.dlq', bad);

        } else {
          // Un fallo de infraestructura debe reintentarse.
          console.error(groupId, 'ERROR', err);
          throw err;
        }
      }

      // Confirmar offset después de procesar el evento.
      await c.commitOffsets([{
        topic,
        partition,
        offset: String(BigInt(offset) + 1n)
      }]);

      // Registro para observabilidad.
      console.log(JSON.stringify({
        servicio: groupId,
        eventId: event?.eventId,
        topic,
        partition,
        offset
      }));
    }
  });

  return { c, dlq };
}
