import {pgTable, bigserial, uuid, text, numeric, timestamp} from 'drizzle-orm/pg-core';

export const marketTrades = pgTable('market_trades', {
    id: bigserial('id', { mode: 'number' }).primaryKey(),
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
    createdAt: timestamp('created_at', {
        withTimezone: true,
    }).notNull().defaultNow()
});