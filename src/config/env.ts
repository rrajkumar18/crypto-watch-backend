import 'dotenv/config';

function required(name:string): string {
    const value = process.env[name];
    if( !value ) {
        throw new Error(`Missing env variable ${name}`);
    }
    return value;
}

export const env = {
    binanceWsUrl: required('BINANCE_WS_URL'),
    binanceSymbols: required('BINANCE_SYMBOLS')
        .split(',')
        .map((symbol) => symbol.trim().toLowerCase())
        .filter(Boolean),
    rabbitmqUrl: required('RABBITMQ_URL'),
    pgUrl: required('PG_URL'),
    timezone: 'Asia/Kolkata',
    jobStartHour: 9,
    jobEndHour: 23,
    pollInterval:5000,
    automaticCollectionMinutes: 2,
    automaticPauseMinutes: 45,
    manualCollectionSeconds: 60,
    tradeBatchSize: 100,
    tradeBatchFlushIntervalMs: 2000,
    tradeMaxBufferSize: 2000,
    // for testing
    // automaticCollectionMinutes: 1,
    // automaticPauseMinutes: 1,
    // manualCollectionSeconds: 30,
}