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
}