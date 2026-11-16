CREATE TYPE "public"."mode" AS ENUM('SCHEDULED', 'MANUAL');--> statement-breakpoint
CREATE TYPE "public"."status" AS ENUM('RUNNING', 'STOPPED', 'SKIPPED', 'FAILED');--> statement-breakpoint
CREATE TABLE "market_sessions" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"mode" varchar(20) NOT NULL,
	"status" "status",
	"started_at" timestamp with time zone NOT NULL,
	"scheduled_stop_at" timestamp with time zone NOT NULL,
	"scheduled_for" timestamp with time zone,
	"stopped_at" timestamp with time zone,
	"stop_reason" varchar(100),
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX "market_sessions_one_running_idx" ON "market_sessions" USING btree ("status") WHERE "market_sessions"."status" = 'RUNNING';--> statement-breakpoint
CREATE UNIQUE INDEX "market_sessions_scheduled_slot_idx" ON "market_sessions" USING btree ("scheduled_for") WHERE "market_sessions"."mode" = 'SCHEDULED' AND "market_sessions"."scheduled_for" IS NOT NULL;