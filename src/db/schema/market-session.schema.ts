import { pgTable, uuid, varchar, timestamp, pgEnum, uniqueIndex } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

export const statusEnum = pgEnum("status", ["RUNNING","STOPPED","SKIPPED","FAILED"]);
export const modeEnum = pgEnum("mode", ["SCHEDULED","MANUAL"]);

export const marketSessions = pgTable('market_sessions', {
    id: uuid("id").defaultRandom().primaryKey(),
    mode: varchar("mode", {
        length:20
    }).notNull(),
    status: statusEnum("status"),
    startedAt: timestamp("started_at", {
        withTimezone: true
    }).notNull(),
    scheduledStopAt: timestamp("scheduled_stop_at", {
        withTimezone: true
    }).notNull(),
    scheduledFor: timestamp("scheduled_for", {
      withTimezone: true,
    }),
    stoppedAt: timestamp("stopped_at", {
        withTimezone: true
    }),
    stopReason: varchar("stop_reason", {
        length:100
    }),
    createdAt: timestamp("created_at", {
        withTimezone: true
    }).defaultNow().notNull()
}, (table) => [
    uniqueIndex("market_sessions_one_running_idx")
    .on(table.status)
    .where(sql`${table.status} = 'RUNNING'`),
    uniqueIndex("market_sessions_scheduled_slot_idx")
      .on(table.scheduledFor)
      .where(sql`${table.mode} = 'SCHEDULED' AND ${table.scheduledFor} IS NOT NULL`),
]);