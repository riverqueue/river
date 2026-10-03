ALTER TABLE /* TEMPLATE: schema */river_job ALTER COLUMN tags SET DEFAULT '{}';
UPDATE /* TEMPLATE: schema */river_job SET tags = '{}' WHERE tags IS NULL;
ALTER TABLE /* TEMPLATE: schema */river_job ALTER COLUMN tags SET NOT NULL;
