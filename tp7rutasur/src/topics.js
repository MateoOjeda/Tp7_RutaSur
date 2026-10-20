import {kafka} from './core.js';
const a=kafka.admin();await a.connect();
try{await a.createTopics({waitForLeaders:true,topics:[
  {topic:'envios.estados',numPartitions:3,configEntries:[{name:'retention.ms',value:'2592000000'}]},
  {topic:'envios.promesas',numPartitions:3,configEntries:[{name:'cleanup.policy',value:'compact'}]},
  {topic:'envios.reprogramaciones',numPartitions:3,configEntries:[{name:'retention.ms',value:'2592000000'}]},
  {topic:'alertas.sla',numPartitions:3,configEntries:[{name:'retention.ms',value:'604800000'}]},
  {topic:'notificaciones.enviadas',numPartitions:3,configEntries:[{name:'retention.ms',value:'604800000'}]},
  {topic:'envios.estados.dlq',numPartitions:3,configEntries:[{name:'retention.ms',value:'2592000000'}]}
]});console.log('Topics disponibles')}finally{await a.disconnect()}
