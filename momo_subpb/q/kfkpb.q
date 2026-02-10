\d .kfkpb

init:`libkfkpb 2:(`kfkpb_init;1);

initConsumer:`libkfkpb 2:(`kfkpb_initConsumer;1);

subscribe:`libkfkpb 2:(`kfkpb_subscribe;2);

cfg:(!) . flip(
    (`metadata.broker.list;`192.168.2.209:9092);
    (`group.id;`0);
    (`fetch.wait.max.ms;`10);
    (`statistics.interval.ms;`10000)
    );

consumecb:{[msg]
 tbl: -9!msg;
 show tbl;
 }

init[];
client:initConsumer[cfg];
subscribe[client;(`futu.basicqot.pb;`futu.ticker.pb;`futu.orderbook.pb;`futu.kl1min.pb)];