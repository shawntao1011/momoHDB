\d .kfkpb

init[];

initConsumer:{[conf]

 }

closeConsumer:{[client]

 }

subscribe:{[h; topics; cbs]

 }

subscribeFromTime:{[h; topics; ts; cbs]

 }

// kafka subscribed topic is bound with client lifecycle
// so kafka unsubscribe can not specify topic, which differs from .u.del
unsubscribe:{

 }

/ k (symbol) - key
/ x (bytes) - kafka message content
upd:{[k; x]
 msg:-9!x;
 }

\d .