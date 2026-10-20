'use client';
import {useEffect,useState} from 'react';
// SSE: recibir señal de cambio y recargar la vista renderizada en servidor.
export default function Live(){const [status,setStatus]=useState('Conectando…');useEffect(()=>{const s=new EventSource('/api/stream');s.onopen=()=>setStatus('En vivo');s.onmessage=(event)=>{try{if(JSON.parse(event.data).changed)window.location.reload()}catch{}};s.onerror=()=>setStatus('Reconectando…');return()=>s.close()},[]);return <small>● {status}</small>}
