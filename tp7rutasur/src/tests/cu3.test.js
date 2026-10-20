
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { producer, publish, mkEvent } from '../core.js';

// =======================================
// CONFIGURACION
// =======================================

const mongo = new MongoClient(
  process.env.MONGO_URI ||
  'mongodb://mongo:27017/rutasur?replicaSet=rs0'
);

const sender = producer();

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

// Los eventos se simulan con una fecha conocida.
// Esto hace que las pruebas sean reproducibles.
const BASE = Date.parse('2026-10-20T12:00:00.000Z');

function fecha(horas) {
  return new Date(BASE + horas * 3600000);
}

function nuevoEnvio(nombre) {
  return `CU3-${nombre}-${randomUUID()}`;
}

// =======================================
// PUBLICAR EVENTOS
// =======================================

async function enviar(topic, id, tipo, secuencia, hora, extra = {}) {
  const evento = mkEvent(
    id,
    tipo,
    secuencia,
    {
      hubId: 'H1',
      zonaId: 'Z1',
      repartidorId: 'R1',
      ...extra
    },
    fecha(hora)
  );

  await publish(sender, topic, evento);
  return evento;
}

// Crear una promesa con vencimiento determinado.
async function promesa(id, vencimiento) {
  return enviar(
    'envios.promesas',
    id,
    'PROMESA',
    0,
    0,
    { venceEn: fecha(vencimiento).toISOString() }
  );
}

// Crear un evento de estado.
async function estado(id, tipo, secuencia, hora, extra = {}) {
  return enviar(
    'envios.estados',
    id,
    tipo,
    secuencia,
    hora,
    extra
  );
}

// =======================================
// ESPERAR RESULTADOS DE MONGODB
// =======================================

// Los consumidores de Kafka son asincronicos.
// Esperamos hasta que la alerta aparezca.
async function esperar(condicion, descripcion, timeout = 45000) {
  const inicio = Date.now();

  while (Date.now() - inicio < timeout) {
    if (await condicion()) return;
    await sleep(300);
  }

  throw new Error(`Tiempo agotado: ${descripcion}`);
}

async function esperarAlerta(db, id, regla, prioridad) {
  let resultado = null;

  await esperar(async () => {
    const alerta = await db.collection('alertas').findOne({
      shipmentId: id,
      regla,
      estado: 'ABIERTA'
    });

    if (!alerta) return false;

    resultado = alerta;
    return true;
  }, `alerta ${regla} del envio ${id}`);

  assert.equal(resultado.prioridad, prioridad);

  return resultado;
}

// =======================================
// PRUEBA 1: SIN INGRESO
// =======================================

async function pruebaSinIngreso(db) {
  const id = nuevoEnvio('SIN-INGRESO');

  // Quedan 8 horas hasta la entrega prometida.
  await promesa(id, 8);
  await estado(id, 'CREADO', 1, 0);

  await esperarAlerta(db, id, 'SIN_INGRESO', 'MEDIA');

  console.log('OK 1/4: SIN_INGRESO -> MEDIA');
}

// =======================================
// PRUEBA 2: SIN SALIDA
// =======================================

async function pruebaSinSalida(db) {
  const id = nuevoEnvio('SIN-SALIDA');

  // Faltan solamente 2 horas.
  await promesa(id, 2);

  await estado(id, 'CREADO', 1, 0);
  await estado(id, 'INGRESADO_HUB', 2, 0);

  await esperarAlerta(db, id, 'SIN_SALIDA', 'ALTA');

  console.log('OK 2/4: SIN_SALIDA -> ALTA');

  return id;
}

// =======================================
// PRUEBA 3: FALLO EN DIA LIMITE
// =======================================

async function pruebaFalloDiaLimite(db) {
  const id = nuevoEnvio('FALLO-LIMITE');

  // Promesa: hoy a las 18:00 UTC.
  // Fallo: hoy a las 12:00 UTC.
  await promesa(id, 6);

  await estado(id, 'CREADO', 1, 0);
  await estado(id, 'INGRESADO_HUB', 2, 0);
  await estado(id, 'EN_REPARTO', 3, 0);

  await estado(
    id,
    'ENTREGA_FALLIDA',
    4,
    0,
    {
      motivo: 'AUSENTE',
      intento: 1,
      franja: 'MAÑANA'
    }
  );

  await esperarAlerta(
    db,
    id,
    'FALLO_DIA_LIMITE',
    'ALTA'
  );

  console.log('OK 3/4: FALLO_DIA_LIMITE -> ALTA');
}

// =======================================
// PRUEBA 4: VENCIDO
// =======================================

async function pruebaVencido(db) {
  const id = nuevoEnvio('VENCIDO');

  // La promesa vencio una hora antes
  // del evento actual.
  await promesa(id, -1);

  await estado(id, 'CREADO', 1, 0);
  await estado(id, 'INGRESADO_HUB', 2, 0);

  await esperarAlerta(db, id, 'VENCIDO', 'CRITICA');

  console.log('OK 4/4: VENCIDO -> CRITICA');
}

// =======================================
// PRUEBA EXTRA: CIERRE DE ALERTAS
// =======================================

async function pruebaCierre(db, id) {
  // El envio de prueba 2 ahora fue entregado.
  await estado(id, 'ENTREGADO', 3, 1, { intento: 1 });

  await esperar(async () => {
    const alerta = await db.collection('alertas').findOne({
      shipmentId: id,
      regla: 'SIN_SALIDA'
    });

    return alerta?.estado === 'CERRADA';
  }, 'cierre de la alerta SIN_SALIDA');

  const alerta = await db.collection('alertas').findOne({
    shipmentId: id,
    regla: 'SIN_SALIDA'
  });

  assert.equal(alerta.estado, 'CERRADA');

  console.log('OK EXTRA: alerta cerrada al entregar');
}

// =======================================
// PRUEBA EXTRA: SIN DUPLICADOS
// =======================================

async function pruebaSinDuplicados(db, id) {
  // La regla SIN_SALIDA solamente puede
  // tener una apertura para este envio.
  const filtro = {
    shipmentId: id,
    regla: 'SIN_SALIDA',
    accion: 'APERTURA'
  };

  const cantidad = await db.collection('sla_emisiones')
    .countDocuments(filtro);

  assert.equal(cantidad, 1);

  console.log('OK EXTRA: sin aperturas duplicadas');
}

// =======================================
// EJECUCION PRINCIPAL
// =======================================

async function main() {
  await mongo.connect();
  await sender.connect();

  const db = mongo.db('rutasur');

  console.log('====================================');
  console.log('INICIANDO PRUEBAS CU3');
  console.log('====================================');

  await pruebaSinIngreso(db);

  const envioSinSalida = await pruebaSinSalida(db);

  await pruebaFalloDiaLimite(db);
  await pruebaVencido(db);

  await pruebaCierre(db, envioSinSalida);
  await pruebaSinDuplicados(db, envioSinSalida);

  console.log('');
  console.log('====================================');
  console.log('CU3: PRUEBAS FUNCIONALES APROBADAS');
  console.log('====================================');
}

// =======================================
// INICIO Y CIERRE DEL PROGRAMA
// =======================================

try {
  await main();
} catch (error) {
  console.error('CU3 FALLO:', error);
  process.exitCode = 1;
} finally {
  await Promise.allSettled([
    sender.disconnect(),
    mongo.close()
  ]);
}
