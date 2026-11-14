import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import pg from 'pg';
import path from 'path';
import { pgPoolConfig } from '../config/config';

async function runMigration() {
	console.log('Creating temporary migration database pool...');
  
  	// Creates an independent, temporary pool for this build process only
  	const migrationPool = new pg.Pool(pgPoolConfig);
  	const migrationDb = drizzle(migrationPool);

  	console.log('Applying database migrations...');
  	const migrationsFolder = path.resolve(__dirname, 'migrations');

  	await migrate(migrationDb, { migrationsFolder }); 

  	console.log('Migrations applied successfully!');
  
  	// Cleanly close this specific process's pool
  	await migrationPool.end(); 
  	console.log('Migration pool closed safely.');
}

runMigration().catch((err) => {
	console.error('Migration failed:', err);
  	process.exit(1);
});
