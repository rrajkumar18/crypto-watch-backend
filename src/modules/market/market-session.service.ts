import { BinanceService } from "../binance/binance.service";
import { SseService } from "../sse/sse.service";
import { MarketSessionRepository } from "../../db/repositories/market-session.repository";
import { SessionMode } from "./market.types";
import { env } from "../../config/env";

export class MarketSessionService {

    private manualStopTimer?: NodeJS.Timeout | undefined;

    constructor(
        private readonly binance: BinanceService,
        private readonly repository: MarketSessionRepository,
        private readonly sse: SseService
    ) {}

    async initialize(): Promise<void> {
        const now = new Date();
        // An expired session cannot be resumed.
        await this.repository.stopExpiredSessions(now);
        const active = await this.repository.findRunning();
        if (!active) {
            console.log("[MarketSession] No active session to recover");
            this.sse.publishCollectorStatus(await this.getStatus());
            return;
        }
        console.log(`[MarketSession] Recovering ${active.mode} session: ${active.id}`);
        // Resume the existing collection session.
        this.binance.start();
        console.log("[MarketSession] Collector recovered");
        this.sse.publishCollectorStatus(await this.getStatus());
    }

    async startManual() {
        const now = new Date();
        const durationMs = Number(env.manualCollectionSeconds) * 1000;
        const stopAt = new Date(now.getTime() + durationMs);
        return this.startSession("MANUAL", stopAt);
    }

    async startScheduled(scheduledFor: Date,stopAt: Date) {
        return this.startSession(
            "SCHEDULED",
            stopAt,
            scheduledFor
        );
    }

    private async startSession(mode: SessionMode, stopAt: Date, scheduledFor?: Date) {
        const active = await this.repository.findRunning();

        if (active) {
            throw new Error(`A collection session is already running (${active.mode})`);
        }

        let session;

        try {
            session = await this.repository.createSession({
                mode,
                status: "RUNNING",
                startedAt: new Date(),
                scheduledStopAt: stopAt,
                scheduledFor: scheduledFor ?? null,
            });
        } catch (error: any) {
            // PostgreSQL unique violation:
            // another request created an active session.
            if (error.code === "23505") {
                throw new Error("A collection session is already running");
            }
            throw error;
        }

        try {
            this.binance.start();
            if (mode === "MANUAL") {
                this.manualStopTimer = setTimeout(() => {
                    console.log("[MarketSession] Manual duration completed. Stopping...");
                    void this.stopCurrent("DURATION_COMPLETED");
                }, Math.max(0, stopAt.getTime() - Date.now()));
            }
            console.log(`[MarketSession] ${mode} collection started. Stop at ${stopAt.toISOString()}`);
            this.sse.publishCollectorStatus(await this.getStatus());
            return session;
        } catch (error) {
            await this.repository.failSession(session?.id as string,new Date());
            throw error;
        }
    }

    async stopCurrent(reason: string = "MANUAL_STOP") {
        const active = await this.repository.findRunning();
        if (!active) {
            return null;
        }
        const stopped = await this.repository.stopSession(active.id, new Date(), reason);
        if (stopped) {
            if (this.manualStopTimer) {
                clearTimeout(this.manualStopTimer);
                this.manualStopTimer = undefined;
            }
            await this.binance.stop();
            console.log(`[MarketSession] Session stopped: ${active.id}`);
            this.sse.publishCollectorStatus(await this.getStatus());
        }
        return stopped;
    }

    async stopIfExpired(): Promise<void> {
        const active = await this.repository.findRunning();
        if (!active) return;

        if (Date.now() >= active.scheduledStopAt.getTime()) {
            await this.stopCurrent("DURATION_COMPLETED");
        }
    }    

    async getStatus() {
        const active = await this.repository.findRunning();
        if (active) {
            return {
                collectorStatus: "RUNNING",
                sessionMode: active.mode,
                sessionId: active.id,
                startedAt: active.startedAt,
                stopsAt: active.scheduledStopAt,
                remainingSeconds: Math.max(0,Math.ceil((active.scheduledStopAt.getTime() - Date.now()) / 1000)),
            };
        }
        const latest = await this.repository.findLatest();
        return {
            collectorStatus: "IDLE",
            lastSession: latest,
        };
    }
}
