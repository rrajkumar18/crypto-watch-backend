import { Server } from "http";
import { createApp } from "./app";

import { RabbitMQService } from "./rabbit-mq/rabbitmq.service";
import { BinanceService } from "./modules/binance/binance.service";
import { MarketTradeWorker } from "./workers/market-trade.worker";

import { MarketSessionRepository } from "./db/repositories/market-session.repository";
import { MarketSessionService } from "./modules/market/market-session.service";
import { MarketSchedulerService } from "./modules/market/market-scheduler.service";
import { SseService } from "./modules/sse/sse.service";

(async function bootstrap() {

    const PORT = Number(process.env.PORT || 4000);

    const rabbitMQ = new RabbitMQService();
    const sse = new SseService();
    const worker = new MarketTradeWorker(sse);

    let server: Server | undefined;
    let scheduler: MarketSchedulerService | undefined;
    let binance: BinanceService | undefined;

    let shuttingDown = false;

    try {

        sse.start();
        await rabbitMQ.connect();
        await worker.start();
        binance = new BinanceService(rabbitMQ);

        const repository = new MarketSessionRepository();
        const sessions = new MarketSessionService(binance, repository, sse);
        await sessions.initialize();

        scheduler = new MarketSchedulerService(sessions,repository);
        scheduler.start();

        const app = createApp(sse, sessions);
        server = app.listen(PORT, () => {
            console.log(`Server running on port ${PORT}`);
        });

        console.log("[Application] Startup complete");

        const shutdown = async () => {
            if (shuttingDown) return;
            shuttingDown = true;
            console.log("\n[Application] Shutting down...");
            scheduler?.stop();            
            try {
                await binance?.stop();
                sse.stop();
                if (server) {
                    await new Promise<void>((resolve, reject) => {
                        server!.close((error) => {
                            if (error) reject(error);
                            else resolve();
                        });
                    });
                }            
                await worker.stop();
                await rabbitMQ.close();
                console.log("[Application] Shutdown complete");
                process.exit(0);
            } catch (error) {
                console.error("[Application] Shutdown error:", error);
                process.exit(1);
            }
        };

        process.once("SIGINT", shutdown);
        process.once("SIGTERM", shutdown);

    } catch (error) {
        console.error("[Application] Startup failed:", error);
        scheduler?.stop();
        await binance?.stop().catch(console.error);
        sse.stop();
        await worker.stop().catch(console.error);
        await rabbitMQ.close().catch(console.error);
        process.exit(1);
    }

})();
