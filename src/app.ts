import express from "express";
import swaggerUi from "swagger-ui-express";
import cors from "cors";
import { swaggerSpec } from "./docs/swagger";
import { SseService } from "./modules/sse/sse.service";
import { MarketSessionService } from "./modules/market/market-session.service";
import { createMarketRouter } from "./routes/route";

export function createApp(sse: SseService, sessions:MarketSessionService) {
    const app = express();
    app.use(express.json());
    app.use(cors({
        // origin: "http://localhost:3000",
        // origin: "*",
        credentials: true,
        methods: ["GET", "OPTIONS"]
    }));

    app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));
    app.use("/api/market", createMarketRouter(sse, sessions));

    return app;
}