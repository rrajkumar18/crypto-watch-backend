import { MarketTradeEvent } from "../../modules/binance/binance.types";
import { db } from "../client";
import { sql } from "drizzle-orm";
import * as schema from "../schema/internal";

export class TradeRepository {

    async saveTrades(events: MarketTradeEvent[]): Promise<void> {
        if (events.length === 0) {
            return;
        }

        await db.insert(schema.marketTrades).values(
            events.map((event) => ({
                eventId: event.eventId,
                symbol: event.symbol,
                price: event.price,
                quantity: event.quantity,
                tradeId: event.tradeId,
                eventTime: event.eventTime
            }))
        ).onConflictDoNothing({
            target: schema.marketTrades.eventId,
        });
    }

    // Keep this for existing callers if needed.
    async saveTrade(event: MarketTradeEvent): Promise<void> {
        await this.saveTrades([event]);
    }

    async findHistoricalCandles(symbol: string, interval: "1m" | "5m" | "15m" | "1h", limit: number) {
        const intervalMap = {
            "1m": "1 minute",
            "5m": "5 minutes",
            "15m": "15 minutes",
            "1h": "1 hour",
        };

        const bucketInterval = intervalMap[interval];

        const result = await db.execute(sql`
            SELECT
                bucket AS time,
                (ARRAY_AGG(
                    price ORDER BY event_time ASC, trade_id ASC
                ))[1] AS open,
                MAX(price) AS high,
                MIN(price) AS low,
                (ARRAY_AGG(
                    price ORDER BY event_time DESC, trade_id DESC
                ))[1] AS close
            FROM (
                SELECT
                    date_bin(
                        ${bucketInterval}::interval,
                        event_time,
                        TIMESTAMPTZ '1970-01-01 00:00:00+00'
                    ) AS bucket,
                    price,
                    event_time,
                    trade_id
                FROM market_trades
                WHERE symbol = ${symbol}
            ) AS trades
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT ${limit}
        `);
        return result.rows.reverse();
    }

}
