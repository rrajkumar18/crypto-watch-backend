
import { DateTime } from "luxon";
import { MarketSessionService } from "./market-session.service";
import { MarketSessionRepository } from "../../db/repositories/market-session.repository";
import { env } from "../../config/env";

export class MarketSchedulerService {

    private timer?: NodeJS.Timeout | undefined;
    private isTicking = false;

    constructor(
        private readonly sessions: MarketSessionService,
        private readonly repository: MarketSessionRepository
    ) {}

    start(): void {

        if (this.timer) return;
        console.log('this.timer',this.timer);
        console.log("[Scheduler] Starting market scheduler");
        void this.tick();
        this.timer = setInterval(() => {
            void this.tick();
        }, Number(env.pollInterval));
    }

    stop(): void {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = undefined;
        }
        console.log("[Scheduler] Stopped");
    }

    private async tick(): Promise<void> {
        if (this.isTicking) return;

        this.isTicking = true;

        try {
            console.log("[Scheduler] Tick executed:", new Date().toISOString());
            // Stop any expired collection session.
            await this.sessions.stopIfExpired();
            const window = this.getCurrentCollectionWindow();            

            if (!window) return;

            // Make sure the current time is inside the collection window.
            const now = DateTime.now().setZone(env.timezone);
            if (now.toMillis() < window.start.toMillis() || now.toMillis() >= window.stop.toMillis()) {
                return;
            }

            // Prevent running the same automatic slot twice.
            const existing = await this.repository.findScheduledSlot(
                window.start.toJSDate()
            );

            if (existing) return;

            const active = await this.repository.findRunning();
            if (active) {
                if (active.mode === "MANUAL") {
                    // Manual collection takes priority.
                    // Record this slot as skipped.
                    await this.repository.skipScheduledSlot(
                        window.start.toJSDate(),
                        window.stop.toJSDate(),
                        new Date()
                    );
                    console.log("[Scheduler] Automatic slot skipped due to manual session");
                }
                return;
            }
            await this.sessions.startScheduled(window.start.toJSDate(), window.stop.toJSDate());
        } catch (error) {
            console.error("[Scheduler] Tick error:", error);
        } finally {
            this.isTicking = false;
        }
    }

    private getCurrentCollectionWindow() {
        const now = DateTime.now().setZone(env.timezone);

        // Monday = 1, Sunday = 7.
        // if (now.weekday > 5) {
        //     console.log('Job run only Mon to Friday, exiting');
        //     return null;
        // }

        const startOfDay = now.startOf("day");
        const operatingStart = startOfDay.set({hour: env.jobStartHour, minute: 0, second: 0, millisecond: 0});
        const operatingEnd = startOfDay.set({hour: env.jobEndHour, minute: 0, second: 0, millisecond: 0});        
        // if (now.toMillis() < operatingStart.toMillis() || now.toMillis() >= operatingEnd.toMillis()) {
        //     return null;
        // }

        const elapsedMinutes = now.diff(operatingStart, "minutes").minutes;
        const cycleMinutes = Number(env.automaticCollectionMinutes) + Number(env.automaticPauseMinutes);
        const cycleNumber = Math.floor( elapsedMinutes / cycleMinutes);
        const windowStart = operatingStart.plus({minutes: cycleNumber * cycleMinutes});
        const windowStop = windowStart.plus({minutes: Number(env.automaticCollectionMinutes)});

        // Only start within the actual collection window.
        // if (now.toMillis() < windowStart.toMillis() || now.toMillis() >= windowStop.toMillis()) {
        //     return null;
        // }

        return {
            start: windowStart,
            stop: windowStop,
        };
    }
}
