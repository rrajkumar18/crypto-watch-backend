
import { and, desc, eq, lte } from "drizzle-orm";
import { db } from "../client";
import { marketSessions } from "../schema/market-session.schema";

export class MarketSessionRepository {

    async findRunning() {
        const [session] = await db
            .select()
            .from(marketSessions)
            .where(eq(marketSessions.status, "RUNNING"))
            .limit(1);

        return session ?? null;
    }

    async findScheduledSlot(scheduledFor: Date) {
        const [session] = await db
            .select()
            .from(marketSessions)
            .where(eq(marketSessions.scheduledFor, scheduledFor))
            .limit(1);

        return session ?? null;
    }

    async createSession(data: typeof marketSessions.$inferInsert) {
        const [session] = await db
            .insert(marketSessions)
            .values(data)
            .returning();

        return session;
    }

    async stopSession(id: string, stoppedAt: Date, reason: string) {
        const [session] = await db
            .update(marketSessions)
            .set({
                status: "STOPPED",
                stoppedAt,
                stopReason: reason,
            })
            .where(
                and(
                    eq(marketSessions.id, id),
                    eq(marketSessions.status, "RUNNING")
                )
            )
            .returning();

        return session ?? null;
    }

    async failSession(id: string, stoppedAt: Date) {
        const [session] = await db
            .update(marketSessions)
            .set({
                status: "FAILED",
                stoppedAt,
                stopReason: "COLLECTOR_ERROR",
            })
            .where(
                and(
                    eq(marketSessions.id, id),
                    eq(marketSessions.status, "RUNNING")
                )
            ).returning();
        return session ?? null;
    }

    async skipScheduledSlot(scheduledFor: Date, scheduledStopAt: Date, skippedAt: Date) {
        const [session] = await db
            .insert(marketSessions)
            .values({
                mode: "SCHEDULED",
                status: "SKIPPED",
                startedAt: skippedAt,
                scheduledFor,
                scheduledStopAt,
                stoppedAt: skippedAt,
                stopReason: "MANUAL_CONFLICT",
            }).returning();
        return session;
    }

    async stopExpiredSessions(now: Date) {
        return db
            .update(marketSessions)
            .set({
                status: "STOPPED",
                stoppedAt: now,
                stopReason: "APPLICATION_RESTART",
            })
            .where(
                and(
                    eq(marketSessions.status, "RUNNING"),
                    lte(marketSessions.scheduledStopAt, now)
                )
            ).returning();
    }

    async findLatest() {
        const [session] = await db
            .select()
            .from(marketSessions)
            .orderBy(desc(marketSessions.createdAt))
            .limit(1);
        return session ?? null;
    }
}
