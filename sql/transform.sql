-- ================================================================
-- Transform: Characters
-- ================================================================
-- Flatten RAW characters into staging with bottom-level columns
TRUNCATE TABLE STG.CHARACTERS_FLAT;

INSERT INTO STG.CHARACTERS_FLAT (
  CHARACTER_ID, NAME, STATUS, SPECIES, TYPE, GENDER, IMAGE, URL,
  CREATED_AT, ORIGIN_NAME, ORIGIN_URL, LOCATION_NAME, LOCATION_URL
)
SELECT
  c.value:id::NUMBER,
  c.value:name::STRING,
  c.value:status::STRING,
  c.value:species::STRING,
  c.value:type::STRING,
  c.value:gender::STRING,
  c.value:image::STRING,
  c.value:url::STRING,
  TRY_TO_TIMESTAMP_NTZ(c.value:created::STRING),
  c.value:origin.name::STRING,
  c.value:origin.url::STRING,
  c.value:location.name::STRING,
  c.value:location.url::STRING,
FROM RAW.CHARACTERS_RAW r,
LATERAL FLATTEN(input => r.RESPONSE_VARIANT:results) c;

-- Merge into final MODEL.CHARACTERS (idempotent upsert)
MERGE INTO MODEL.CHARACTERS tgt
USING (SELECT DISTINCT * FROM STG.CHARACTERS_FLAT) src
ON tgt.CHARACTER_ID = src.CHARACTER_ID
WHEN MATCHED THEN UPDATE SET
  NAME = src.NAME, STATUS = src.STATUS, SPECIES = src.SPECIES, TYPE = src.TYPE,
  GENDER = src.GENDER, IMAGE = src.IMAGE, URL = src.URL, CREATED_AT = src.CREATED_AT,
  ORIGIN_NAME = src.ORIGIN_NAME, ORIGIN_URL = src.ORIGIN_URL, LOCATION_NAME = src.LOCATION_NAME, LOCATION_URL = src.LOCATION_URL
WHEN NOT MATCHED THEN
  INSERT VALUES (src.CHARACTER_ID, src.NAME, src.STATUS, src.SPECIES, src.TYPE,
    src.GENDER, src.IMAGE, src.URL, src.CREATED_AT, src.ORIGIN_NAME, src.ORIGIN_URL, src.LOCATION_NAME, src.LOCATION_URL);

-- ================================================================
-- Transform: Episodes
-- ================================================================
-- Flatten RAW episodes into staging
TRUNCATE TABLE STG.EPISODES_FLAT;

INSERT INTO STG.EPISODES_FLAT (
  EPISODE_ID, NAME, AIR_DATE, EPISODE_CODE, URL, CREATED_AT
)
SELECT
  e.value:id::NUMBER,
  e.value:name::STRING,
  e.value:air_date::STRING,
  e.value:episode::STRING,
  e.value:url::STRING,
  TRY_TO_TIMESTAMP_NTZ(e.value:created::STRING),
FROM RAW.EPISODES_RAW r,
LATERAL FLATTEN(input => r.RESPONSE_VARIANT:results) e;

-- Merge into final MODEL.EPISODES (idempotent upsert)
MERGE INTO MODEL.EPISODES tgt
USING (SELECT DISTINCT * FROM STG.EPISODES_FLAT) src
ON tgt.EPISODE_ID = src.EPISODE_ID
WHEN MATCHED THEN UPDATE SET
  NAME = src.NAME, AIR_DATE = src.AIR_DATE, EPISODE_CODE = src.EPISODE_CODE,
  URL = src.URL, CREATED_AT = src.CREATED_AT
WHEN NOT MATCHED THEN
  INSERT VALUES (src.EPISODE_ID, src.NAME, src.AIR_DATE, src.EPISODE_CODE,
    src.URL, src.CREATED_AT);

-- ================================================================
-- Transform: Bridge (Character ↔ Episode)
-- ================================================================
-- Explode character.episode arrays into bridge staging table
TRUNCATE TABLE STG.CHAR_EP_BRIDGE;

INSERT INTO STG.CHAR_EP_BRIDGE (CHARACTER_ID, EPISODE_ID)
SELECT DISTINCT
  c.value:id::NUMBER,
  TO_NUMBER(REGEXP_SUBSTR(ep.value::STRING, '\\d+$'))
FROM RAW.CHARACTERS_RAW r,
LATERAL FLATTEN(input => r.RESPONSE_VARIANT:results) c,
LATERAL FLATTEN(input => c.value:episode) ep;

-- Merge into final MODEL.CHARACTER_EPISODE
MERGE INTO MODEL.CHARACTER_EPISODE tgt
USING (SELECT * FROM STG.CHAR_EP_BRIDGE) src
ON tgt.CHARACTER_ID = src.CHARACTER_ID AND tgt.EPISODE_ID = src.EPISODE_ID
WHEN NOT MATCHED THEN
  INSERT VALUES (src.CHARACTER_ID, src.EPISODE_ID);
