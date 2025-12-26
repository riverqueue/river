ALTER TABLE /* TEMPLATE: schema */river_leader
    DROP CONSTRAINT name_length,
    ADD CONSTRAINT name_length CHECK (name = 'default');