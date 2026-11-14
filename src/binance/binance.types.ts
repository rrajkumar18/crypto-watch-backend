export interface BinanceAggregateTrade {
    e: 'aggTrade';
    E: number;
    s: string;
    a: number;
    p: string;
    q: string;
    f: number;
    l: number;
    T: number;
    m: boolean;
    M: boolean;
}

export interface BinanceCombinedMessage {
    stream: string;
    data: BinanceAggregateTrade;
}

export interface MarketTradeEvent {
    eventId: string;
    source: 'binance';
    eventType: 'trade';
    symbol: string;
    price: string;
    quantity: string;
    tradeId: number;
    eventTime: Date;
    receivedAt: Date;
    buyerIsMaker: boolean;
}
