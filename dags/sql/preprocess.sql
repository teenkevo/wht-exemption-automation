-- One row per entity before feature computation.
-- The platform projects still prepare extracts in Spark. Point this file at
-- the warehouse query when this directory is split into its own repository.

SELECT *
FROM source_table
WHERE entity_id IS NOT NULL
