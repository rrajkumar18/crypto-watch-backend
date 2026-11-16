import { createMarketModuleRouter } from "../modules/market/market.routes";
import { SseService } from "../modules/sse/sse.service";
import { MarketSessionService } from "../modules/market/market-session.service";

export function createMarketRouter(sse: SseService, sessions:MarketSessionService) {
    return createMarketModuleRouter(sse, sessions);
}