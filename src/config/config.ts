import 'dotenv/config';
import type { PoolConfig } from 'pg';

export const pgPoolConfig: PoolConfig = {
    connectionString: process.env.PG_URL,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
};