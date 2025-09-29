-- ================================================================
-- Setup: Warehouse and Database
-- ================================================================
-- Create and use a dedicated compute warehouse
CREATE WAREHOUSE IF NOT EXISTS RICKMORTY_WH
  WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
USE WAREHOUSE RICKMORTY_WH;

-- Create and use a dedicated database
CREATE DATABASE IF NOT EXISTS RICKMORTY_DB;
USE DATABASE RICKMORTY_DB;

-- ================================================================
-- Schemas
-- ================================================================
-- RAW schema: stores untouched JSON from API
CREATE SCHEMA IF NOT EXISTS RAW;

-- STG schema: flattened intermediate tables
CREATE SCHEMA IF NOT EXISTS STG;

-- MODEL schema: final normalized tables
CREATE SCHEMA IF NOT EXISTS MODEL;

-- ================================================================
-- RAW Tables
-- ================================================================
-- Stores raw JSON response per page for characters
CREATE TABLE IF NOT EXISTS RAW.CHARACTERS_RAW (
  PAGE_NUMBER NUMBER,
  RESPONSE_VARIANT VARIANT,
  FETCHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stores raw JSON response per page for episodes
CREATE TABLE IF NOT EXISTS RAW.EPISODES_RAW (
  PAGE_NUMBER NUMBER,
  RESPONSE_VARIANT VARIANT,
  FETCHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ================================================================
-- STAGING Tables
-- ================================================================
-- Flattened characters with nested objects pulled into columns
CREATE OR REPLACE TABLE STG.CHARACTERS_FLAT (
  CHARACTER_ID NUMBER,
  NAME VARCHAR,
  STATUS VARCHAR,
  SPECIES VARCHAR,
  TYPE VARCHAR,
  GENDER VARCHAR,
  IMAGE VARCHAR,
  URL VARCHAR,
  CREATED_AT TIMESTAMP_NTZ,
  ORIGIN_NAME VARCHAR,
  ORIGIN_URL VARCHAR,
  LOCATION_NAME VARCHAR,
  LOCATION_URL VARCHAR,
  INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Flattened episodes with top-level fields only
CREATE OR REPLACE TABLE STG.EPISODES_FLAT (
  EPISODE_ID NUMBER,
  NAME VARCHAR,
  AIR_DATE VARCHAR,
  EPISODE_CODE VARCHAR,
  URL VARCHAR,
  CREATED_AT TIMESTAMP_NTZ,
  INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Bridge table linking characters to episodes (many-to-many)
CREATE OR REPLACE TABLE STG.CHAR_EP_BRIDGE (
  CHARACTER_ID NUMBER,
  EPISODE_ID NUMBER,
  INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ================================================================
-- MODEL Tables
-- ================================================================
-- Final characters table
CREATE OR REPLACE TABLE MODEL.CHARACTERS (
  CHARACTER_ID NUMBER PRIMARY KEY,
  NAME VARCHAR NOT NULL,
  STATUS VARCHAR,
  SPECIES VARCHAR,
  TYPE VARCHAR,
  GENDER VARCHAR,
  IMAGE VARCHAR,
  URL VARCHAR,
  CREATED_AT TIMESTAMP_NTZ,
  ORIGIN_NAME VARCHAR,
  ORIGIN_URL VARCHAR,
  LOCATION_NAME VARCHAR,
  LOCATION_URL VARCHAR
);

-- Final episodes table
CREATE OR REPLACE TABLE MODEL.EPISODES (
  EPISODE_ID NUMBER PRIMARY KEY,
  NAME VARCHAR NOT NULL,
  AIR_DATE VARCHAR,
  EPISODE_CODE VARCHAR,
  URL VARCHAR,
  CREATED_AT TIMESTAMP_NTZ
);

-- Final bridge table (character ↔ episode)
CREATE OR REPLACE TABLE MODEL.CHARACTER_EPISODE (
  CHARACTER_ID NUMBER,
  EPISODE_ID NUMBER,
  PRIMARY KEY (CHARACTER_ID, EPISODE_ID)
);
