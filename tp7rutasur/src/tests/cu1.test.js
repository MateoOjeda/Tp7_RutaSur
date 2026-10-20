
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { kafka, mkEvent, producer, publish } from '../core.js';

// Esperar una cantidad de milisegundos.
const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

// Identificador único del envío de prueba.
const shipmentId = `CU1-${randomUUID()}`;

// Conexión a MongoDB.
const mongo = new MongoClient(
  process.env.MONGO_URI ||
  'mongodb://mongo:27017/rutasur?replicaSet=rs0'
);

// Productor de Kafka.
const sender = producer();

// Consumidor exclusivo para comprobar la DLQ.
const dlqReader = kafka.consumer({
  groupId: `prueba-cu1-${randomUUID()}`
});

const TOPIC = 'envios.estados';

// Esperar hasta que se cumpla una condición.
async function waitFor(
  check,
  description,
  timeoutMs = 30000
) {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    if (await check()) {
      return;
    }

    await sleep(300);
  }

  throw new Error(
    `Tiempo agotado esperando: ${description}`
  );
}

// Función principal de las pruebas.
async function main() {
  await mongo.connect();
  await sender.connect();

  const db = mongo.db('rutasur');
  const envios = db.collection('envios');

  // Escuchar los eventos inválidos enviados a la DLQ.
  let receivedDlq = null;

  await dlqReader.connect();

  await dlqReader.subscribe({
    topic: 'envios.estados.dlq',
    fromBeginning: true
  });

  await dlqReader.run({
    eachMessage: async ({ message }) => {
      try {
        const event = JSON.parse(
          message.value.toString()
        );

        if (event.shipmentId === shipmentId) {
          receivedDlq = event;
        }
      } catch {
        // Ignorar mensajes que no se puedan interpretar.
      }
    }
  });

  console.log(`Envío de prueba: ${shipmentId}`);

  // Estados del ciclo de vida.
  const types = [
    'CREADO',
    'INGRESADO_HUB',
    'CLASIFICADO',
    'EN_REPARTO',
    'ENTREGADO'
  ];

  // Crear los eventos con secuencias del 1 al 5.
  const events = types.map((tipo, index) =>
    mkEvent(shipmentId, tipo, index + 1, {
      hubId: 'H1',
      zonaId: 'Z1',
      repartidorId: 'R1',
      ...(tipo === 'ENTREGADO'
        ? { intento: 1 }
        : {})
    })
  );

  // =====================================
  // PRUEBA 1: CICLO DE VIDA COMPLETO
  // =====================================

  for (const event of events) {
    await publish(sender, TOPIC, event);
  }

  await waitFor(async () => {
    const doc = await envios.findOne({ shipmentId });

    return (
      doc?.estado === 'ENTREGADO' &&
      doc.historial?.length === 5
    );
  }, 'estado ENTREGADO e historial completo');

  console.log('OK 1/4: ciclo de vida completo');

  // =====================================
  // PRUEBA 2: EVENTO DUPLICADO
  // =====================================

  // Volver a publicar el primer evento.
  await publish(sender, TOPIC, events[0]);

  await sleep(1200);

  let doc = await envios.findOne({ shipmentId });

  assert.equal(doc.estado, 'ENTREGADO');
  assert.equal(doc.historial.length, 5);

  console.log('OK 2/4: evento duplicado descartado');

  // =====================================
  // PRUEBA 3: EVENTO FUERA DE ORDEN
  // =====================================

  // Crear un evento nuevo con secuencia antigua.
  const oldEvent = mkEvent(
    shipmentId,
    'CLASIFICADO',
    2
  );

  await publish(sender, TOPIC, oldEvent);

  await sleep(1500);

  doc = await envios.findOne({ shipmentId });

  assert.equal(doc.estado, 'ENTREGADO');
  assert.equal(doc.ultimaSecuencia, 5);
  assert.equal(doc.historial.length, 5);

  console.log('OK 3/4: evento fuera de orden descartado');

  // =====================================
  // PRUEBA 4: EVENTO INVÁLIDO Y DLQ
  // =====================================

  // La secuencia debe ser un número.
  // Enviamos texto intencionalmente.
  const malformed = {
    ...mkEvent(shipmentId, 'CREADO', 6),
    secuencia: 'incorrecta'
  };

  await publish(sender, TOPIC, malformed);

  await waitFor(
    () => receivedDlq !== null,
    'evento inválido en la DLQ'
  );

  assert.equal(receivedDlq.origen, TOPIC);
  assert.ok(receivedDlq.razon);

  console.log('OK 4/4: evento inválido enviado a DLQ');

  console.log('');
  console.log('===================================');
  console.log('CU1: PRUEBAS DE BACKEND APROBADAS');
  console.log(`Envío: ${shipmentId}`);
  console.log('===================================');
}

// Ejecutar el programa.
try {
  await main();
} catch (error) {
  console.error('CU1 FALLÓ:', error.message);
  process.exitCode = 1;
} finally {
  // Cerrar las conexiones.
  await Promise.allSettled([
    dlqReader.disconnect(),
    sender.disconnect(),
    mongo.close()
  ]);
}
