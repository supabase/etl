-- Remove publication_name column from pipelines table since it's redundant
-- (already stored in the config JSONB column)
alter table app.pipelines drop column publication_name;