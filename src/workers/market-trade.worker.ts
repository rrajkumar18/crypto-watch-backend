import amqp, {Channel, ChannelModel, ConsumeMessage} from 'amqplib';
import { env } from '../config/env';
import { MarketTradeEvent } from '../binance/binance.types';
import { TradeRepository } from '../db/repositories/trade.repository';

const EXCHANGE = 'crypto.events';
const RETRY_EXCHANGE = 'crypto.events.retry';
const TRADE_QUEUE = 'crypto.market.trade';
const TRADE_ROUTING_KEY = 'market.trade';

const DLQ_QUEUE = 'crypto.market.trade.dlq';
const DLQ_ROUTING_KEY = 'market.trade.dlq';

type retryQueue = {
    queue:string;
    routingKey:string;
    delay:number
}

const RETRY_QUEUES:Array<retryQueue> = [
    {
        queue: 'crypto.market.trade.retry.1',
        routingKey: 'market.trade.retry.1',
        delay: 5_000
    },
    {
        queue: 'crypto.market.trade.retry.2',
        routingKey: 'market.trade.retry.2',
        delay: 30_000
    },
    {
        queue: 'crypto.market.trade.retry.3',
        routingKey: 'market.trade.retry.3',
        delay: 120_000
    }
];

const MAX_RETRIES = RETRY_QUEUES.length;

export class MarketTradeWorker {

    private connection?:ChannelModel;
    private channel?:Channel;
    private trade = new TradeRepository();

    async start():Promise<void> {
        console.log('[Worker] Connecting to RabbitMQ...');
        this.connection = await amqp.connect(env.rabbitmqUrl);
        this.channel = await this.connection.createChannel();
        
        await this.setupRabbitMQ();

        // Don't allow this consumer to receive
        // an unlimited number of unacknowledged messages.
        await this.channel.prefetch(100);

        await this.channel.consume(TRADE_QUEUE, (message) => {
            if( !message ) {
                return;
            }
            void this.processMessage(message);
        }, {noAck: false});

        console.log('[Worker] Listening for market trades');
    }

    private async setupRabbitMQ():Promise<void> {
        const channel = this.channel;

        if( !channel ) {
            throw new Error('RabbitMQ channel is not initialized');
        }

        // Main Exchange
        await channel.assertExchange(EXCHANGE, 'topic', {durable: true});
        // Re-try Exchange
        await channel.assertExchange(RETRY_EXCHANGE, 'topic', {durable: true});
        // Dead-letter Queue
        await channel.assertQueue(DLQ_QUEUE, {durable: true});

        await channel.bindQueue(DLQ_QUEUE, EXCHANGE, DLQ_ROUTING_KEY);
        
        /*
         * Main trade queue.
         * Messages that expire from retry queues are sent
         * back to the main exchange with routing key: market.trade
         */
        await channel.assertQueue(TRADE_QUEUE, {durable: true});
        await channel.bindQueue(TRADE_QUEUE, EXCHANGE, TRADE_ROUTING_KEY);

        // Retry queue
        for( const retry of RETRY_QUEUES ) {
            await channel.assertQueue(retry.queue, {
                durable: true,
                arguments: {
                    // When the TTL expires, send the message
                    // to the main exchange.
                    'x-dead-letter-exchange': EXCHANGE,
                    'x-dead-letter-routing-key': TRADE_ROUTING_KEY
                }
            });

            await channel.bindQueue(retry.queue, RETRY_EXCHANGE, retry.routingKey);
        }

    }

    private async processMessage(message:ConsumeMessage):Promise<void> {
        try {
            const event = JSON.parse(message.content.toString()) as MarketTradeEvent;
            console.log(
                `[Worker] ${event.symbol} ` +
                `price=${event.price} ` +
                `quantity=${event.quantity}`,
            );
            // Database processing will go here later.
            this.trade.saveTrade(event);
            this.channel?.ack(message);
        } catch (error) {
            console.error(
                '[Worker] Failed to process message:',
                error,
            );
            // this.channel?.nack(message, false, false);
            await this.retryMessage(message);
        }
    }

    private async retryMessage(message: ConsumeMessage): Promise<void> {
        const channel = this.channel;

        if (!channel) {
            return;
        }

        const retryCount = this.getRetryCount(message);

        /*
         * We have exhausted all retry attempts.
         */
        if (retryCount >= RETRY_QUEUES.length) {
            console.error(
                `[Worker] Message failed after ${retryCount} retries. ` +
                'Sending to DLQ.',
            );

            channel.publish(
                EXCHANGE,
                DLQ_ROUTING_KEY,
                message.content,
                {
                    persistent: true,
                    headers: {
                        ...message.properties.headers,
                        'x-retry-count': retryCount,
                    },
                },
            );
            channel.ack(message);
            return;
        } else {

            const retry = RETRY_QUEUES[retryCount];

            if (!retry) {
                console.error(
                    `[Worker] No retry queue configured for retry count ${retryCount}`,
                );

                channel.ack(message);

                return;
            }

            const nextRetryCount = retryCount + 1;

            console.log(
                `[Worker] Scheduling retry ${nextRetryCount}/${MAX_RETRIES} ` +
                `in ${retry?.delay}ms`,
            );

            /*
            * Publish to the retry queue.
            *
            * expiration is per-message TTL.
            */
            channel.publish(RETRY_EXCHANGE, retry.routingKey, message.content,{
                persistent: true,
                expiration: retry.delay.toString(),
                headers: {
                    ...message.properties.headers,
                    'x-retry-count': nextRetryCount,
                }
            });

            /*
            * ACK the original message only after publishing
            * the retry message.
            */
            channel.ack(message);
        }
    }

    private getRetryCount(message: ConsumeMessage): number {
        const value = message.properties.headers?.['x-retry-count'];
        if (typeof value === 'number') {
            return value;
        }
        if (typeof value === 'string') {
            const parsed = Number(value);
            return Number.isNaN(parsed) ? 0 : parsed;
        }
        return 0;
    }

    async stop(): Promise<void> {
        await this.channel?.close();
        await this.connection?.close();
    }

}