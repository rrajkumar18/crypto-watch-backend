CREATE TABLE "market_trades" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"event_id" uuid NOT NULL,
	"symbol" text NOT NULL,
	"price" numeric(30, 10) NOT NULL,
	"quantity" numeric(30, 10) NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "market_trades_event_id_unique" UNIQUE("event_id")
);
