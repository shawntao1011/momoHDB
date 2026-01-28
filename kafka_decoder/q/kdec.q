\d .kdec

init:{

 }

consumer:{[conf]

 }

// Subscribe to a topic from a client, with a defined topic/partition offset and unique callback function
/* client = Integer denoting client Id
/* topics = Topic to be subscribed to as a symbol, or list of topics (as symbol list)
/* part = Partition list or partition/offset dictionary (depreciated / unused)
/* cb   = callback function to be used for the specified topic
subscribe:{[client; topics; partition; callbacks]
 .kfk.Sub[cid;top;part];
 }

close:{[client]

 }

statsf:{

 }

////////////////////////
//     GLOBAL INFO    //
////////////////////////

stats:();