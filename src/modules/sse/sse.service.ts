import { Response } from "express";
import { MarketTradeEvent } from "../binance/binance.types";

interface LivePrice {
    symbol: string;
    price: string;
    quantity: string;
    eventTime: string;
    receivedAt: string;
    tradeId: number;
}

interface SseClient {
    id: number;
    response: Response;
}

export class SseService {

    private clients = new Map<number, SseClient>();
    private latestPrices = new Map<string, LivePrice>();

    private latestCollectorStatus: unknown = {
        collectorStatus: "IDLE",
        sessionMode: null,
        remainingSeconds: 0,
    };

    private nextClientId = 1;

    private broadcastTimer?: NodeJS.Timeout | undefined;
    private heartbeatTimer?: NodeJS.Timeout | undefined;

    private readonly broadcastIntervalMs = 1000;
    private readonly heartbeatIntervalMs = 25000;

    start(): void {
        if (this.broadcastTimer) return;

        console.log("[SSE] Starting broadcast service");

        this.broadcastTimer = setInterval(() => {
            this.broadcastLatestPrices();
        }, this.broadcastIntervalMs);

        this.heartbeatTimer = setInterval(() => {
            this.sendHeartbeat();
        }, this.heartbeatIntervalMs);
    }

    connect(response: Response): void {
        const id = this.nextClientId++;
        response.status(200);
        response.set({
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            // "Access-Control-Allow-Origin": "http://localhost:3000",
            "Access-Control-Allow-Origin": "*",
        });
        response.flushHeaders();

        const client: SseClient = {
            id,
            response,
        };

        this.clients.set(id, client);
        console.log(`[SSE] Client connected. Total clients: ${this.clients.size}`);

        // Send initial snapshot.
        this.send(client, "connected", {
            message: "Connected to live market stream",
            connectedAt: new Date().toISOString(),
        });

        this.send(client, "prices", {
            prices: Array.from(this.latestPrices.values()),
            timestamp: new Date().toISOString(),
        });

        this.send(client, "collector-status", this.latestCollectorStatus);

        response.on("close", () => {
            this.clients.delete(id);
            console.log(`[SSE] Client disconnected. Total clients: ${this.clients.size}`);
        });
    }

    publishTrades(events: MarketTradeEvent[]): void {
        for (const event of events) {
            const current = this.latestPrices.get(event.symbol);
            const eventTime = new Date(String(event.eventTime));
            const receivedAt = new Date(String(event.receivedAt));
            if ( Number.isNaN(eventTime.getTime()) || Number.isNaN(receivedAt.getTime()) ) {
                console.error("[SSE] Invalid trade timestamps:", event.eventId);
                continue;
            }

            // Don't replace a newer price with an older event.
            if (current && eventTime.getTime() <= new Date(current.eventTime).getTime()) {
                continue;
            }
            
            this.latestPrices.set(event.symbol, {
                symbol: event.symbol,
                price: event.price,
                quantity: event.quantity,
                eventTime: new Date(event.eventTime).toISOString(),
                receivedAt: new Date(event.receivedAt).toISOString(),
                tradeId: event.tradeId,
            });
        }
    }

    publishCollectorStatus(status: unknown): void {
        this.latestCollectorStatus = status;
        this.broadcast("collector-status", status);
    }

    private broadcastLatestPrices(): void {
        if (this.clients.size === 0 || this.latestPrices.size === 0) {
            return;
        }
        const payload = {
            prices: Array.from(this.latestPrices.values()),
            timestamp: new Date().toISOString(),
        };
        this.broadcast("prices", payload);
    }

    private sendHeartbeat(): void {
        this.broadcast("heartbeat", {
            timestamp: new Date().toISOString(),
        });
    }

    private broadcast(event: string, data: unknown): void {        
        const message =
            `event: ${event}\n` +
            `data: ${JSON.stringify(data)}\n\n`;

        for (const client of this.clients.values()) {
            try {
                client.response.write(message);
            } catch (error) {
                console.error(`[SSE] Failed to send to client ${client.id}:`,error);
                this.clients.delete(client.id);
            }
        }
    }

    private send(client: SseClient, event: string, data: unknown): void {
        client.response.write(
            `event: ${event}\n` +
            `data: ${JSON.stringify(data)}\n\n`
        );
    }

    getClientCount(): number {
        return this.clients.size;
    }

    stop(): void {
        if (this.broadcastTimer) {
            clearInterval(this.broadcastTimer);
            this.broadcastTimer = undefined;
        }
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = undefined;
        }
        for (const client of this.clients.values()) {
            client.response.end();
        }
        this.clients.clear();
        console.log("[SSE] Broadcast service stopped");
    }
}
