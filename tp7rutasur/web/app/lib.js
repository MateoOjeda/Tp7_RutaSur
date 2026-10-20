import {MongoClient} from 'mongodb';
const uri=process.env.MONGO_URI||'mongodb://mongo:27017/rutasur?replicaSet=rs0';
const client=new MongoClient(uri);let connect;
export async function db(){connect ||= client.connect();await connect;return client.db('rutasur')}
export async function getEnvios(limit=30){return (await db()).collection('envios').find({}).sort({_id:-1}).limit(limit).toArray()}
export async function getAlertas(){return (await db()).collection('alertas').find({estado:'ABIERTA'}).sort({prioridad:1}).limit(60).toArray()}
export async function shipment(id){if(!/^RS-[A-Za-z0-9-]{1,80}$/.test(id))return null;return (await db()).collection('envios').findOne({shipmentId:id})}
