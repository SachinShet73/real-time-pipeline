-- Create user if not exists
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'dataeng') THEN
      CREATE ROLE dataeng LOGIN PASSWORD 'password123';
   END IF;
END
$do$;

-- Grant privileges
ALTER ROLE dataeng WITH SUPERUSER;
ALTER USER dataeng WITH PASSWORD 'password123';

-- Create database if not exists
CREATE DATABASE pipeline_db WITH OWNER = dataeng;

-- Connect to the database
\c pipeline_db;

-- Grant privileges on database
GRANT ALL PRIVILEGES ON DATABASE pipeline_db TO dataeng;

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL PRIVILEGES ON SCHEMA public TO dataeng;

-- Alter default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO dataeng;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO dataeng;