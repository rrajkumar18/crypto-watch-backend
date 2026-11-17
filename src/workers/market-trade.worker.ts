import amqp, {ConfirmChannel, ChannelModel, ConsumeMessage} from "amqplib";
import { env } from "../config/env";
import { MarketTradeEvent } from "../modules/binance/binance.types";
import { TradeRepository } from "../db/repositories/trade.repository";
import { SseService } from "../modules/sse/sse.service";

const EXCHANGE = "crypto.events";
const RETRY_EXCHANGE = "crypto.events.retry";

const TRADE_QUEUE = "crypto.market.trade";
const TRADE_ROUTING_KEY = "market.trade";

const DLQ_QUEUE = "crypto.market.trade.dlq";
const DLQ_ROUTING_KEY = "market.trade.dlq";

interface TradeBatch {
    batchId: string;
    events: MarketTradeEvent[];
    createdAt: string;
}

interface RetryQueue {
    queue: string;
    routingKey: string;
    delay: number;
}

const RETRY_QUEUES: RetryQueue[] = [
    {
        queue: "crypto.market.trade.retry.1",
        routingKey: "market.trade.retry.1",
        delay: 5_000,
    },
    {
        queue: "crypto.market.trade.retry.2",
        routingKey: "market.trade.retry.2",
        delay: 30_000,
    },
    {
        queue: "crypto.market.trade.retry.3",
        routingKey: "market.trade.retry.3",
        delay: 120_000,
    }
];

export class MarketTradeWorker {

    private connection?: ChannelModel;
    private channel?: ConfirmChannel;
    private trade = new TradeRepository();

    constructor(private readonly sse:SseService){

    }

    async start(): Promise<void> {
        console.log("[Worker] Connecting to RabbitMQ...");
        this.connection = await amqp.connect(env.rabbitmqUrl);
        this.channel = await this.connection.createConfirmChannel();
        await this.setupRabbitMQ();

        // Limit unacknowledged batch messages.
        await this.channel.prefetch(10);

        await this.channel.consume(TRADE_QUEUE,
            (message) => {
                if (!message) return;
                void this.processMessage(message);
            },
            {
                noAck: false 
            }
        );
        console.log("[Worker] Listening for trade batches");
    }

    private async setupRabbitMQ(): Promise<void> {
        const channel = this.channel;
        if (!channel) {
            throw new Error("RabbitMQ channel is not initialized");
        }

        await channel.assertExchange(EXCHANGE,"topic",{ durable: true });
        await channel.assertExchange(RETRY_EXCHANGE,"topic",{ durable: true });

        await channel.assertQueue(DLQ_QUEUE,{ durable: true });
        await channel.bindQueue(DLQ_QUEUE,EXCHANGE,DLQ_ROUTING_KEY);
        
        await channel.assertQueue(TRADE_QUEUE,{ durable: true });
        await channel.bindQueue(TRADE_QUEUE,EXCHANGE,TRADE_ROUTING_KEY);

        for (const retry of RETRY_QUEUES) {
            await channel.assertQueue(retry.queue, {
                durable: true,
                arguments: {
                    "x-dead-letter-exchange": EXCHANGE,
                    "x-dead-letter-routing-key": TRADE_ROUTING_KEY,
                },
            });
            await channel.bindQueue(retry.queue, RETRY_EXCHANGE, retry.routingKey);
        }
    }

    private async processMessage(message: ConsumeMessage): Promise<void> {
        try {
            const payload = JSON.parse(message.content.toString()) as TradeBatch | MarketTradeEvent;

            // Accept old single-trade messages as well as new batches.
            const rawEvents = "events" in payload ? payload.events : [payload];

            if (!Array.isArray(rawEvents) || rawEvents.length === 0) {
                throw new Error("Invalid or empty trade batch");
            }

            const events: MarketTradeEvent[] = rawEvents.map((event) => ({
                ...event,
                eventTime: new Date(event.eventTime),
                receivedAt: new Date(event.receivedAt),
            }));            

            // console.log(`[Worker] Processing batch of ${events.length} trades`);
            await this.trade.saveTrades(events);
            // ACK only after the database insertion succeeds.
            this.channel?.ack(message);

            try {
                // Broadcast Trades to client
                this.sse.publishTrades(events);
                // console.log(`[Worker] Successfully saved ${events.length} trades`);
            } catch (err) {
                console.error("[Worker] SSE broadcast failed:", err);
            }
            
        } catch (error) {
            console.error("[Worker] Failed to process trade batch:",error);
            await this.retryMessage(message);
        }
    }

    private async retryMessage(message: ConsumeMessage): Promise<void> {
        const channel = this.channel;
        if (!channel) return;

        const retryCount = this.getRetryCount(message);

        if (retryCount >= RETRY_QUEUES.length) {
            console.error(`[Worker] Batch failed after ${retryCount} retries. Sending to DLQ.`);
            const writable = channel.publish(
                EXCHANGE,
                DLQ_ROUTING_KEY,
                message.content,
                {
                    persistent: true,
                    contentType: message.properties.contentType,
                    headers: {
                        ...message.properties.headers,
                        "x-retry-count": retryCount,
                    },
                }
            );
            if (!writable) {
                console.warn("[Worker] DLQ publisher buffer is full");
            }
            await channel.waitForConfirms();
            channel.ack(message);
            return;
        }

        const retry = RETRY_QUEUES[retryCount];
        if (!retry) {
            throw new Error("No retry queue configured");
        }

        const nextRetryCount = retryCount + 1;
        console.log(`[Worker] Scheduling batch retry ${nextRetryCount}/${RETRY_QUEUES.length} in ${retry.delay}ms`);

        const writable = channel.publish(
            RETRY_EXCHANGE,
            retry.routingKey,
            message.content,
            {
                persistent: true,
                expiration: retry.delay.toString(),
                contentType: message.properties.contentType,
                headers: {
                ...message.properties.headers,
                "x-retry-count": nextRetryCount,
                },
            }
        );

        if (!writable) {
            console.warn("[Worker] Retry publisher buffer is full");
        }

        // Confirm the retry message before acknowledging original.
        await channel.waitForConfirms();
        channel.ack(message);
    }

    private getRetryCount(message: ConsumeMessage): number {
        const value = message.properties.headers?.["x-retry-count"];
        if (typeof value === "number") {
            return value;
        }
        if (typeof value === "string") {
            const parsed = Number(value);
            return Number.isNaN(parsed) ? 0 : parsed;
        }
        return 0;
    }

    async stop(): Promise<void> {
        await this.channel?.close();
        await this.connection?.close();
        console.log("[Worker] Stopped");
    }
}
