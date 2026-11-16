import amqp, { ConfirmChannel,ChannelModel } from "amqplib";
import { env } from "../config/env";
import { MarketTradeEvent } from "../modules/binance/binance.types";

const EXCHANGE = "crypto.events";
const TRADE_QUEUE = "crypto.market.trade";
const TRADE_ROUTING_KEY = "market.trade";

interface TradeBatch {
    batchId: string;
    events: MarketTradeEvent[];
    createdAt: string;
}

export class RabbitMQService {

    private connection?: ChannelModel;
    private channel?: ConfirmChannel;

    private tradeBuffer: MarketTradeEvent[] = [];

    private flushTimer?: NodeJS.Timeout | undefined;
    private flushPromise?: Promise<void> | undefined;

    private isClosing = false;

    async connect(): Promise<void> {
        this.connection = await amqp.connect(env.rabbitmqUrl);

        this.connection.on("error", (error) => {
            console.error("[RabbitMQ] Connection error:", error);
        });

        this.connection.on("close", () => {
            console.warn("[RabbitMQ] Connection closed");
        });

        // ConfirmChannel allows us to wait for broker confirmations.
        this.channel = await this.connection.createConfirmChannel();
        await this.setupTopology();
        this.startFlushTimer();
        console.log("[RabbitMQ] Connected");
    }

    private async setupTopology(): Promise<void> {
        if (!this.channel) {
            throw new Error("RabbitMQ channel not initialized");
        }

        await this.channel.assertExchange(EXCHANGE, "topic", { durable: true });
        await this.channel.assertQueue(TRADE_QUEUE, { durable: true });
        await this.channel.bindQueue(TRADE_QUEUE, EXCHANGE, TRADE_ROUTING_KEY);
    }

    async publishTrade(event: MarketTradeEvent): Promise<void> {
        if (!this.channel || this.isClosing) {
            throw new Error("RabbitMQ is not accepting messages");
        }

        this.tradeBuffer.push(event);

        if (this.tradeBuffer.length >= env.tradeBatchSize) {
            await this.flushTrades("BATCH_SIZE");
        }
    }

    private startFlushTimer(): void {
        this.flushTimer = setInterval(() => {
            if (this.tradeBuffer.length > 0) {
            void this.flushTrades("TIMER").catch((error) => {
                console.error("[RabbitMQ] Scheduled flush failed:", error);
            });
            }
        }, env.tradeBatchFlushIntervalMs);
    }

    async flushTrades(reason: "BATCH_SIZE" | "TIMER" | "SHUTDOWN" = "TIMER"): Promise<void> {

        // Serialize concurrent flush attempts.
        if (this.flushPromise) {
            return this.flushPromise;
        }

        if (!this.channel || this.tradeBuffer.length === 0) {
            return;
        }

        this.flushPromise = this.performFlush(reason);

        try {
            await this.flushPromise;
        } finally {
            this.flushPromise = undefined;
        }
    }

    private async performFlush(reason: "BATCH_SIZE" | "TIMER" | "SHUTDOWN"): Promise<void> {
        const channel = this.channel;
        if (!channel || this.tradeBuffer.length === 0) {
            return;
        }

        const batchSize = Math.min(env.tradeBatchSize, this.tradeBuffer.length);
        const events:Array<any> = this.tradeBuffer.splice(0, batchSize);

        const batch: TradeBatch = {
            batchId: `${events[0].eventId}-${events[events.length - 1].eventId}`,
            events,
            createdAt: new Date().toISOString(),
        };

        const message = Buffer.from(JSON.stringify(batch));

        try {

            const writable = channel.publish(
                EXCHANGE,
                TRADE_ROUTING_KEY,
                message,
                {
                    persistent: true,
                    contentType: "application/json",
                    contentEncoding: "utf-8",
                    messageId: batch.batchId,
                    timestamp: Date.now(),
                    type: "trade.batch",
                    headers: {
                        source: "binance",
                        batchSize: events.length,
                    }
                }
            );

            if (!writable) {
                console.warn("[RabbitMQ] Write buffer full; waiting for confirms");
            }

            // Wait until RabbitMQ confirms the published batch.
            await channel.waitForConfirms();

            // console.log(`[RabbitMQ] Published batch: ${events.length} trades | Reason: ${reason}`);

        } catch (error) {
            // Restore unconfirmed trades to the front of the buffer.
            this.tradeBuffer.unshift(...events);
            console.error("[RabbitMQ] Batch publishing failed:", error);
            throw error;
        }
    }

    async close(): Promise<void> {
        this.isClosing = true;
        if (this.flushTimer) {
            clearInterval(this.flushTimer);
            this.flushTimer = undefined;
        }

        // Flush remaining trades before closing RabbitMQ.
        await this.flushTrades("SHUTDOWN");
        await this.channel?.close();
        await this.connection?.close();
        console.log("[RabbitMQ] Closed");
    }
}
