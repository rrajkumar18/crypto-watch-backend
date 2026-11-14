import amqp, { Channel, ChannelModel, Options} from 'amqplib';
import { env } from '../config/env';
import { MarketTradeEvent } from '../binance/binance.types';

const EXCHANGE = 'crypto.events';
const TRADE_QUEUE = 'crypto.market.trade';
const TRADE_ROUTING_KEY = 'market.trade';

export class RabbitMQService {

    private connection?:ChannelModel;
    private channel?:Channel;

    async connect():Promise<void> {
        this.connection = await amqp.connect(env.rabbitmqUrl);

        this.connection.on('error', (error) => {
            console.error('[RabbitMQ] Connection error:', error);
        });

        this.connection.on('close', () => {
            console.warn(`[RabbitMQ] connection closed`);
        });

        this.channel = await this.connection.createChannel();
        await this.setupTopology();
        console.log('[RabbitMQ] Connected');
    }

    private async setupTopology():Promise<void> {
        if( !this.channel ) {
            throw new Error('RabbitMQ channel not initialized');
        }        
        await this.channel.assertExchange(EXCHANGE, 'topic', {durable:true});
        await this.channel.assertQueue(TRADE_QUEUE, {durable:true});
        await this.channel.bindQueue(TRADE_QUEUE, EXCHANGE, TRADE_ROUTING_KEY);
    }

    async publishTrade(event:MarketTradeEvent):Promise<void> {        
        if( !this.channel ) {
            throw new Error('Rabbit MQ is not connected');
        }
        
        const message = Buffer.from(JSON.stringify(event));
        const published = this.channel.publish(EXCHANGE, TRADE_ROUTING_KEY, message, {
            persistent: true,
            contentType: 'application/json',
            contentEncoding: 'utf-8',
            messageId: event.eventId,
            timestamp: Date.now(),
            type: event.eventType,
            headers: {
                source: event.source
            }
        });

        if( !published ) {
            console.warn(`[RabbitMQ] Write buffer is full`);
        }
    }

    async close():Promise<void> {
        await this.channel?.close();
        await this.connection?.close();
    }

}