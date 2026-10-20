import './style.css';
export const metadata={title:'RutaSur | Torre de control',description:'Tracking logístico con Kafka'};
export default function Layout({children}){return <html lang="es"><body><header><b>📦 RutaSur</b><nav><a href="/">Torre de control</a><a href="/analitica">Analítica</a><a href="/demo">Demo</a></nav></header><main>{children}</main></body></html>}
