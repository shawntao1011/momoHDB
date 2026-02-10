
Quotes: @[;`sym;`p#]
    ([] sym             :0#`;
        time            :0Np;
        spread          :0Nf;
        high            :0Nf;
        open            :0Nf;
        low             :0Nf;
        cur             :0Nf;
        lastclose       :0Nf;
        volume          :0Nj;
        amount          :0Nf;
        turnoverrate    :0Nf;
        amplitude       :0Nf;
        updtime         :0Np
    );

Minutes: @[;`sym;`p#]
    ([] sym             :0#`;
        time            :0Np;
        high            :0Nf;
        open            :0Nf;
        low             :0Nf;
        close           :0Nf;
        lastclose       :0Nf;
        volume          :0Nj;
        amount          :0Nf;
        turnoverrate    :0Nf;
        pe              :0Nf;
        changerate      :0Nf;
        recvtime        :0Np;
        tstime          :0Np //timestamp
    );

Ticks: @[;`sym;`p#]
    ([] sym             :0#`;
        time            :0Np;
        direction       :`;
        price           :0Nf;
        volume          :0Nj;
        msgtime         :0Np;
        amount          :0Nf;
        recvtime        :0Np
    );

OrderBooks: @[;`sym;`p#]
    ([] sym             :0#`;
        time            :0Np;
        side            :`;
        level           :0Nh; //rank
        price           :0Nf;
        volume          :0Nj;
        ordercount      :0Ni
    );

