import { Router } from "express";
import { SseService } from "../sse/sse.service";
import { getHistoricalCandles } from "./market.controller";
import { MarketSessionService } from "./market-session.service";

export function createMarketModuleRouter(sse: SseService, sessions: MarketSessionService) {
    const router = Router();

    /**
     * @swagger
     * /market/history:
     *   get:
     *     summary: Get historical graph data
     *     description: Get historical graph data
     *     tags:
     *       - Market Collection
     *     responses:
     *       200:
     *         description: data points fetched     
     */
    // Get historical market trade data for charts
    router.get("/history", getHistoricalCandles);

    /**
     * @swagger
     * /market/start:
     *   post:
     *     summary: Start manual market collection
     *     description: Starts a manual Binance market data collection session. The session automatically stops after the configured manual collection duration.
     *     tags:
     *       - Market Collection
     *     responses:
     *       201:
     *         description: Market collection started successfully
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *               properties:
     *                 message:
     *                   type: string
     *                   example: Market collection started
     *                 session:
     *                   $ref: '#/components/schemas/MarketSession'
     *       409:
     *         description: A collection session is already running
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/ApiError'
     *       500:
     *         description: Internal server error
     */

    // Start binance API manually
    router.post("/start", async (req, res) => {
        try {
            const session = await sessions.startManual();
            res.status(201).json({
                message: "Market collection started",
                session,
            });
        } catch (error) {
            const message = error instanceof Error ? error.message : "Unable to start market collection";
            res.status(409).json({ message });
        }
    });

    /**
     * @swagger
     * /market/stop:
     *   post:
     *     summary: Stop market collection
     *     description: Stops the currently running manual or scheduled market collection session.
     *     tags:
     *       - Market Collection
     *     responses:
     *       200:
     *         description: Market collection stopped successfully
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *               properties:
     *                 message:
     *                   type: string
     *                   example: Market collection stopped
     *                 session:
     *                   $ref: '#/components/schemas/MarketSession'
     *       409:
     *         description: No active collection session
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/ApiError'
     *       500:
     *         description: Internal server error
     */
    // Stop binance API manually
    router.post("/stop", async (req, res) => {
        try {
            const session = await sessions.stopCurrent();
            if (!session) {
                res.status(409).json({
                    message: "No active collection session",
                });
                return;
            }
            res.json({
                message: "Market collection stopped",
                session,
            });
        } catch (error) {
            console.error("[Binance API] Stop failed:", error);
            res.status(500).json({
                message: "Unable to stop market collection",
            });
        }
    });

    /**
     * @swagger
     * /market/stream:
     *   get:
     *     summary: Stream Live prices from Binance
     *     description: Stream Live prices from Binance.
     *     tags:
     *       - Market Collection    
     */
    router.get("/stream", (req, res) => {
        req.setTimeout(0);
        res.setTimeout(0);
        sse.connect(res);
    });

    return router;
}