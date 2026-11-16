ALTER TABLE "market_trades" ADD COLUMN "trade_id" bigint NOT NULL;--> statement-breakpoint
ALTER TABLE "market_trades" ADD COLUMN "event_time" timestamp with time zone NOT NULL;