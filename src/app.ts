import express from "express";
import swaggerUi from "swagger-ui-express";
import cors from "cors";
import { swaggerSpec } from "./docs/swagger";
import { SseService } from "./modules/sse/sse.service";
import { MarketSessionService } from "./modules/market/market-session.service";
import { createMarketRouter } from "./routes/route";

export function createApp(sse: SseService, sessions:MarketSessionService) {
    const app = express();
    const allowedOrigins = [
        'http://localhost:3000',
        'https://url-shortner-frontend-nine-sigma.vercel.app',
    ];    
    app.use(cors({
        origin: (origin, callback) => {
            if (!origin || allowedOrigins.includes(origin)) {
                callback(null, true);
            } else {
                callback(new Error(`Origin ${origin} not allowed by CORS`));
            }
        },
        credentials: true,
        methods: ['GET', 'POST', 'OPTIONS'],
        allowedHeaders: ['Content-Type', 'Authorization'],
    }));
    app.use(express.json());

    app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));
    app.use("/api/market", createMarketRouter(sse, sessions));

    return app;
}