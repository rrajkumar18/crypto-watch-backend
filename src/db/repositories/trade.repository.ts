import { MarketTradeEvent } from "../../binance/binance.types";
import { db } from "./../client";
import * as schema from "./../schema/internal";

export class TradeRepository {

    async saveTrade(event: MarketTradeEvent): Promise<void> {
        await db.insert(schema.marketTrades).values({
            eventId : event.eventId,
            symbol : event.symbol,
            price : event.price,
            quantity : event.quantity,
        });
    }
}