import {notFound} from 'next/navigation';import {shipment} from '../../lib';import Live from '../../components/Live';
export const dynamic='force-dynamic';
export default async function Page({params}){const {id}=await params;const e=await shipment(id);if(!e)notFound();return <><h1>Seguimiento de envío</h1><div className="box"><p>Código: {e.shipmentId}</p><h2>Estado: {e.estado.replaceAll('_',' ')}</h2><p>Entrega estimada: {e.promesa?.venceEn?new Date(e.promesa.venceEn).toLocaleString('es-AR'):'Pendiente de estimación'}</p><Live/></div></>}
