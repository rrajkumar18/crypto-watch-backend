import { drizzle } from 'drizzle-orm/node-postgres';
import pg from 'pg';
import { pgPoolConfig } from '../config/config';

const pool = new pg.Pool(pgPoolConfig);

pool.on('error', (err) => {
    console.error('Unexpected error on idle PostgreSQL client', err);
});

export const db = drizzle(pool);