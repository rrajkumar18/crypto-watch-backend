import { RabbitMQService } from "./rabbit-mq/rabbitmq.service";
import { BinanceService } from "./binance/binance.service";
import { MarketTradeWorker } from "./workers/market-trade.worker.js";
import { Server } from "http";
import app from "./app";

(async function() {

    const PORT = process.env.PORT || 3000;
    const rabbitMQ = new RabbitMQService();
    const worker = new MarketTradeWorker();

    let server:Server;

    try {

        await rabbitMQ.connect();
        await worker.start();

        // const binanceService = new BinanceService(rabbitMQ);
        // binanceService.start();

        server = app.listen(PORT, () => {
            console.log(`Server running on port ${PORT}`);
        });

        const shutdown = async () => {
            console.log('\nShutting down...');
            // binanceService.stop();
            server.close(async () => {
                try {
                    await rabbitMQ.close();
                    process.exit(0);
                } catch (error) {
                    console.error("Shutdown error:", error);
                    process.exit(1);
                }
            });
            await rabbitMQ.close();
        };

        process.on('SIGINT', shutdown);
        process.on('SIGTERM', shutdown);
    } catch (error) {
        console.error("Application startup failed:", error);
        await rabbitMQ.close().catch(console.error);
        process.exit(1);
    }

})().catch(e => {
    console.log(e);
});