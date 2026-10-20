
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { mkEvent, producer, publish } from '../core.js';

// =====================================
// CONFIGURACION
// =====================================

const mongo = new MongoClient(
  process.env.MONGO_URI ||
  'mongodb://mongo:27017/rutasur?replicaSet=rs0'
);

const sender = producer();
const sleep = ms => new Promise(r => setTimeout(r, ms));

// Generamos un identificador único para aislar esta prueba.
const runId = randomUUID().slice(0, 8);

// Usamos una zona y un hub exclusivos de esta ejecución.
// Así no mezclamos resultados con los 965 envíos anteriores.
const zonaId = `Z-CU4-${runId}`;
const hubId = `H-CU4-${runId}`;

// Fecha y franja fijas para comprobar los cálculos.
const fecha = '2026-10-20';
const franja = 'TARDE';

// Esperar el procesamiento asíncrono de Kafka.
async function esperar(condicion, descripcion, limite = 45000) {
  const inicio = Date.now();

  while (Date.now() - inicio < limite) {
    if (await condicion()) return;
    await sleep(300);
  }

  throw new Error(`Tiempo agotado: ${descripcion}`);
}

// Crear y publicar un evento de estado.
async function enviar(id, tipo, secuencia, extra = {}) {
  const evento = mkEvent(
    id,
    tipo,
    secuencia,
    {
      hubId,
      zonaId,
      repartidorId: 'R1',
      franja,
      ...extra
    },
    new Date('2026-10-20T16:00:00.000Z')
  );

  await publish(sender, 'envios.estados', evento);
  return evento;
}

// Preparar un recorrido básico de un envío.
async function preparar(id) {
  await enviar(id, 'CREADO', 1);
  await enviar(id, 'INGRESADO_HUB', 2);
  await enviar(id, 'CLASIFICADO', 3);
  await enviar(id, 'EN_REPARTO', 4);
}

// Consultar los contadores de esta corrida.
async function obtenerMetricas(db) {
  return db.collection('metricas_diarias').findOne({
    fecha,
    zonaId,
    hubId,
    franja
  });
}

async function main() {
  await mongo.connect();
  await sender.connect();

  const db = mongo.db('rutasur');

  console.log('====================================');
  console.log('INICIANDO PRUEBAS CU4');
  console.log('====================================');
  console.log(`Zona de prueba: ${zonaId}`);

  // =====================================
  // ENVIO A: ENTREGADO EN PRIMER INTENTO
  // =====================================

  const envioA = `CU4-A-${randomUUID()}`;

  await preparar(envioA);

  await enviar(envioA, 'ENTREGADO', 5, {
    intento: 1,
    venceEn: '2026-10-20T20:00:00.000Z'
  });

  // =====================================
  // ENVIO B: ENTREGA FALLIDA POR AUSENCIA
  // =====================================

  const envioB = `CU4-B-${randomUUID()}`;

  await preparar(envioB);

  await enviar(envioB, 'ENTREGA_FALLIDA', 5, {
    intento: 1,
    motivo: 'AUSENTE'
  });

  // =====================================
  // ENVIO C: ENTREGADO EN SEGUNDO INTENTO
  // Y FUERA DEL PLAZO PROMETIDO
  // =====================================

  const envioC = `CU4-C-${randomUUID()}`;

  await preparar(envioC);

  await enviar(envioC, 'ENTREGADO', 5, {
    intento: 2,
    venceEn: '2026-10-20T14:00:00.000Z'
  });

  // Esperamos los resultados de MongoDB.
  await esperar(async () => {
    const m = await obtenerMetricas(db);

    return (
      m?.intentos === 3 &&
      m?.entregados === 2 &&
      m?.fallos === 1 &&
      m?.entregasPrimerIntento === 1
    );
  }, 'contadores de analítica');

  const m = await obtenerMetricas(db);

  // =====================================
  // PRUEBA 1: PRIMER INTENTO
  // =====================================

  assert.equal(m.entregados, 2);
  assert.equal(m.entregasPrimerIntento, 1);

  const tasaPrimerIntento =
    (m.entregasPrimerIntento / m.entregados) * 100;

  assert.equal(tasaPrimerIntento, 50);

  console.log(
    `OK 1/3: primer intento = ${tasaPrimerIntento}%`
  );

  // =====================================
  // PRUEBA 2: MOTIVOS DE FALLO
  // =====================================

  assert.equal(m.fallos, 1);
  assert.equal(m.motivos?.AUSENTE, 1);

  console.log('OK 2/3: un fallo por AUSENTE');

  // =====================================
  // PRUEBA 3: CUMPLIMIENTO DE SLA
  // =====================================

  assert.equal(m.fallosSLA, 1);

  const entregasEnPlazo = m.entregados - m.fallosSLA;
  const cumplimiento = (entregasEnPlazo / m.entregados) * 100;

  assert.equal(cumplimiento, 50);

  console.log(
    `OK 3/3: cumplimiento de SLA = ${cumplimiento}%`
  );

  console.log('');
  console.log('====================================');
  console.log('CU4: TRES INDICADORES APROBADOS');
  console.log('====================================');
  console.log('Pendiente: indicador histórico mediante replay.');
}

try {
  await main();
} catch (error) {
  console.error('CU4 FALLÓ:', error);
  process.exitCode = 1;
} finally {
  await Promise.allSettled([
    sender.disconnect(),
    mongo.close()
  ]);
}
