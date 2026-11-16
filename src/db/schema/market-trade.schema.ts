import {pgTable, bigserial, bigint, text, numeric, timestamp} from 'drizzle-orm/pg-core';

export const marketTrades = pgTable('market_trades', {
    id: bigserial('id', { 
        mode: 'number'
    }).primaryKey(),
    eventId: text('event_id').notNull().unique(),
    symbol: text('symbol').notNull(),
    price: numeric('price', {
        precision: 30,
        scale: 10,
    }).notNull(),
    quantity: numeric('quantity', {
        precision: 30,
        scale: 10,
    }).notNull(),
    tradeId: bigint('trade_id', {
        mode: 'number',
    }).notNull(),
    eventTime: timestamp("event_time", {
        withTimezone: true,
    }).notNull(),
    createdAt: timestamp('created_at', {
        withTimezone: true,
    }).notNull().defaultNow()
});