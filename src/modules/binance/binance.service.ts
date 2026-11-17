import WebSocket from "ws";
import { env } from "../../config/env";
import { BinanceAggregateTrade, MarketTradeEvent } from "./binance.types";
import { RabbitMQService } from "../../rabbit-mq/rabbitmq.service";

export class BinanceService {

    private socket?:WebSocket | undefined;
    private reconnectTimer?:NodeJS.Timeout | undefined;
    private readonly reconnectDelay = 5000;
    private isRunning = false;

    constructor( private readonly rabbitMQ: RabbitMQService) {
        
    }

    start():void {
        if (this.isRunning) {
            console.log("[Binance] Already running");
            return;
        }
        this.isRunning = true;
        this.connect();
    }

    private connect():void {
        if(!this.isRunning) return;

        console.log('[Binance] >>> start() ENTERED');
        const streams = env.binanceSymbols.map((symbol) => `${symbol.toLocaleLowerCase()}@aggTrade`);
        let url = env.binanceWsUrl;
        if( streams.length == 1 ) {
            url = `${url}/ws/${streams[0]}`;
        } else {
            url = `${url}/stream?streams=${streams.join('/')}`;
        }

        console.log(`[Binance] Start Conncetion...`);
        console.log(`[Binance] Streams: ${streams}`);

        this.socket = new WebSocket(url);

        console.log('[Binance] socket created');
        console.log('[Binance] readyState:', this.socket.readyState);
        console.log('[Binance] url:', this.socket.url);

        this.socket.on('open',() => {
            console.log(`[Binance] Connected`);
        });

        this.socket.on('message',(raw) => {
            // console.log(`[Binance] received message`);
            this.handleMessage(raw.toString());
        });

        this.socket.on('error', (error) => {
            console.error('[Binance] WebSocket error:', error.message);
        });

        this.socket.on('close', (code, reason) => {
            console.warn(`[Binance] Connection closed. code=${code} reason=${reason.toString()}`);
            this.scheduleReconnect();
        });

        this.socket.on("unexpected-response", (request, response) => {
            console.error(
                "Unexpected HTTP response:",
                response.statusCode,
                response.statusMessage
            );
        });
    }

    private handleMessage(message:string):void {
        try {
            const parsed = JSON.parse(message);
            const data = parsed.data as BinanceAggregateTrade;
            void this.handleTrade(data);
        } catch (error) {
            console.error('[Binance] Failed to parse message:', error);
        }
    }

    private async handleTrade(event:BinanceAggregateTrade):Promise<void> {
        const receivedAt = new Date();
        const tradeEvent: MarketTradeEvent = {
            eventId: `binance-${event.s}-${event.a}`,
            source: 'binance',
            eventType: 'trade',
            symbol: event.s,
            price: event.p,
            quantity: event.q,
            tradeId: event.a,
            eventTime: new Date(event.T),
            receivedAt: receivedAt,
            buyerIsMaker: event.m,
        };
        try {
            await this.rabbitMQ.publishTrade(tradeEvent);
        } catch (error) {
            console.error('[Binance] Failed to publish trade:', error);
        }
    }

    private scheduleReconnect():void {
        if (!this.isRunning || this.reconnectTimer) {
            return;
        }
        console.log(`[Binance] Reconnecting in ${this.reconnectDelay / 1000}s...`);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = undefined;
            if (this.isRunning) {
                this.connect();
            }
        }, this.reconnectDelay);
    }

    async stop(): Promise<void> {
        this.isRunning = false;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = undefined;
        }

        const socket = this.socket;
        this.socket = undefined;

        if ( socket && socket.readyState !== WebSocket.CLOSED ) {
            await new Promise<void>((resolve) => {
                const timeout = setTimeout(resolve, 2000);
                socket.once("close", () => {
                    clearTimeout(timeout);
                    resolve();
                });
                socket.close();
            });
        }

        // Flush incomplete batches before stopping collection.
        await this.rabbitMQ.flushTrades();
        console.log("[Binance] Stopped and pending trades flushed");
    }

}