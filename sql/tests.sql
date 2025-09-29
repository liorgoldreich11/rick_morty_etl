-- Check uniqueness of character IDs
SELECT COUNT(*) - COUNT(DISTINCT CHARACTER_ID) AS duplicate_characters
FROM MODEL.CHARACTERS;

-- Check uniqueness of episode IDs
SELECT COUNT(*) - COUNT(DISTINCT EPISODE_ID) AS duplicate_episodes
FROM MODEL.EPISODES;

-- Check uniqueness of character-episode pairs
SELECT COUNT(*) - COUNT(DISTINCT CHARACTER_ID || '-' || EPISODE_ID) AS duplicate_pairs
FROM MODEL.CHARACTER_EPISODE;

-- Check referential integrity: no orphan characters
SELECT COUNT(*) AS orphan_characters
FROM MODEL.CHARACTER_EPISODE ce
LEFT JOIN MODEL.CHARACTERS c ON ce.CHARACTER_ID = c.CHARACTER_ID
WHERE c.CHARACTER_ID IS NULL;

-- Check referential integrity: no orphan episodes
SELECT COUNT(*) AS orphan_episodes
FROM MODEL.CHARACTER_EPISODE ce
LEFT JOIN MODEL.EPISODES e ON ce.EPISODE_ID = e.EPISODE_ID
WHERE e.EPISODE_ID IS NULL;
