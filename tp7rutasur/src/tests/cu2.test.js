
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { producer, publish, mkEvent } from '../core.js';

// Configuración
const mongo = new MongoClient(
  process.env.MONGO_URI ||
  'mongodb://mongo:27017/rutasur?replicaSet=rs0'
);

const sender = producer();
const TOPIC = 'envios.estados';
const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

// Esperar a que los consumidores procesen un evento.
async function waitFor(check, description, timeout = 45000) {
  const start = Date.now();

  while (Date.now() - start < timeout) {
    if (await check()) return;
    await sleep(300);
  }

  throw new Error(`Tiempo agotado: ${description}`);
}

// Crear un envío nuevo para evitar datos de pruebas anteriores.
function newId(name) {
  return `CU2-${name}-${randomUUID()}`;
}

// Enviar un evento válido a Kafka.
async function send(id, tipo, secuencia, extra = {}) {
  const event = mkEvent(id, tipo, secuencia, {
    hubId: 'H1',
    zonaId: 'Z1',
    repartidorId: 'R1',
    ...extra
  });

  await publish(sender, TOPIC, event);
  return event;
}

// Preparar el ciclo inicial de un paquete.
async function prepare(id) {
  await send(id, 'CREADO', 1);
  await send(id, 'INGRESADO_HUB', 2);
  await send(id, 'CLASIFICADO', 3);
  await send(id, 'EN_REPARTO', 4, { intento: 1 });
}

// Consultar una notificación asociada al evento original.
async function notificationCount(db, eventId) {
  return db.collection('notificaciones')
    .countDocuments({ origen: eventId });
}

// Verificar las tres reacciones de un evento.
async function verifyFailure(db, id, failure, action) {
  const envios = db.collection('envios');

  await waitFor(async () => {
    const doc = await envios.findOne({ shipmentId: id });
    const notices = await notificationCount(db, failure.eventId);

    return (
      doc?.estado === 'ENTREGA_FALLIDA' &&
      doc?.reprogramacion?.accion === action &&
      notices === 1
    );
  }, `${id}: estado, notificación y ${action}`);

  const doc = await envios.findOne({ shipmentId: id });

  assert.equal(doc.estado, 'ENTREGA_FALLIDA');
  assert.equal(doc.reprogramacion.accion, action);
  assert.equal(
    await notificationCount(db, failure.eventId),
    1
  );

  return doc;
}

// PRUEBA 1: AUSENTE
async function testAbsent(db) {
  const id = newId('AUSENTE');

  await prepare(id);

  const failure = await send(
    id,
    'ENTREGA_FALLIDA',
    5,
    {
      motivo: 'AUSENTE',
      intento: 1,
      franja: 'MAÑANA'
    }
  );

  const doc = await verifyFailure(
    db, id, failure, 'REPROGRAMAR'
  );

  // El fallo fue por la mañana.
  // El siguiente intento debe ser por la tarde.
  assert.equal(doc.reprogramacion.franja, 'TARDE');

  // La fecha de reprogramación debe ser un día hábil posterior.
  const next = new Date(doc.reprogramacion.fecha);
  const original = new Date(failure.ocurridoEn);

  assert.ok(next > original);
  assert.ok(![0, 6].includes(next.getUTCDay()));

  console.log('OK 1/4: AUSENTE -> REPROGRAMAR');
}

// PRUEBA 2: DIRECCIÓN INCORRECTA
async function testBadAddress(db) {
  const id = newId('DIRECCION');

  await prepare(id);

  const failure = await send(
    id,
    'ENTREGA_FALLIDA',
    5,
    {
      motivo: 'DIRECCION_INCORRECTA',
      intento: 1,
      franja: 'TARDE'
    }
  );

  const doc = await verifyFailure(
    db, id, failure, 'RETENER'
  );

  assert.equal(doc.reprogramacion.fecha, null);

  console.log('OK 2/4: DIRECCION_INCORRECTA -> RETENER');
}

// PRUEBA 3: RECHAZADO
async function testRejected(db) {
  const id = newId('RECHAZADO');

  await prepare(id);

  const failure = await send(
    id,
    'ENTREGA_FALLIDA',
    5,
    {
      motivo: 'RECHAZADO',
      intento: 1,
      franja: 'MAÑANA'
    }
  );

  await verifyFailure(db, id, failure, 'DEVOLVER');

  console.log('OK 3/4: RECHAZADO -> DEVOLVER');
}

// PRUEBA 4: TERCER INTENTO FALLIDO
async function testThirdFailure(db) {
  const id = newId('TERCER-FALLO');

  await prepare(id);

  let seq = 5;

  // Primer fallo.
  const failure1 = await send(
    id, 'ENTREGA_FALLIDA', seq++,
    { motivo: 'AUSENTE', intento: 1, franja: 'MAÑANA' }
  );

  // Segundo intento.
  await send(id, 'EN_REPARTO', seq++, { intento: 2 });

  const failure2 = await send(
    id, 'ENTREGA_FALLIDA', seq++,
    { motivo: 'AUSENTE', intento: 2, franja: 'TARDE' }
  );

  // Tercer intento.
  await send(id, 'EN_REPARTO', seq++, { intento: 3 });

  const failure3 = await send(
    id, 'ENTREGA_FALLIDA', seq++,
    { motivo: 'AUSENTE', intento: 3, franja: 'MAÑANA' }
  );

  const doc = await verifyFailure(
    db, id, failure3, 'DEVOLVER'
  );

  assert.equal(doc.intentosFallidos, 3);

  // Los tres intentos generan avisos independientes.
  await waitFor(async () =>
    (await notificationCount(db, failure1.eventId)) === 1 &&
    (await notificationCount(db, failure2.eventId)) === 1 &&
    (await notificationCount(db, failure3.eventId)) === 1,
    'tres notificaciones'
  );

  console.log('OK 4/4: tercer fallo -> DEVOLVER');
}

// Ejecución principal.
async function main() {
  await mongo.connect();
  await sender.connect();

  const db = mongo.db('rutasur');

  console.log('===================================');
  console.log('INICIANDO PRUEBAS CU2');
  console.log('===================================');

  await testAbsent(db);
  await testBadAddress(db);
  await testRejected(db);
  await testThirdFailure(db);

  console.log('');
  console.log('===================================');
  console.log('CU2: 4 PRUEBAS FUNCIONALES APROBADAS');
  console.log('===================================');
}

try {
  await main();
} catch (error) {
  console.error('CU2 FALLÓ:', error);
  process.exitCode = 1;
} finally {
  await Promise.allSettled([
    sender.disconnect(),
    mongo.close()
  ]);
}
