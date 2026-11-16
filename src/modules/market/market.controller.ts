import { Request, Response } from "express";
import { TradeRepository } from "../../db/repositories/trade.repository";

const tradeRepository = new TradeRepository();

const VALID_INTERVALS = ["1m", "5m", "15m", "1h"] as const;

type CandleInterval = typeof VALID_INTERVALS[number];

export async function getHistoricalCandles(req: Request, res: Response): Promise<void> {
    try {
        const symbol = String(req.query.symbol ?? "").toUpperCase();
        const interval = String(req.query.interval ?? "1m");
        const limit = Number(req.query.limit ?? 500);

        // Validate symbol
        if (!/^[A-Z0-9]+$/.test(symbol)) {
            res.status(400).json({
                message: "Invalid or missing symbol",
            });
            return;
        }

        // Validate interval
        if (!VALID_INTERVALS.includes(interval as CandleInterval)) {
            res.status(400).json({
                message: "Invalid interval. Supported intervals: 1m, 5m, 15m, 1h",
            });
            return;
        }

        // Validate limit
        if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
            res.status(400).json({
                message: "Limit must be an integer between 1 and 1000",
            });
            return;
        }

        const candles = await tradeRepository.findHistoricalCandles(
            symbol,
            interval as CandleInterval,
            limit
        );

        const formattedCandles = candles.map((candle) => ({
            time: Math.floor(new Date(candle.time as string).getTime() / 1000),
            open: Number(candle.open),
            high: Number(candle.high),
            low: Number(candle.low),
            close: Number(candle.close),
        }));

        res.status(200).json({
            symbol,
            interval,
            candles: formattedCandles,
        });
    } catch (error) {
        console.error("Failed to fetch historical candles:", error);

        res.status(500).json({
            message: "Failed to fetch historical market data",
        });
    }
}