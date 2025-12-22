ALTER TABLE /* TEMPLATE: schema */river_leader
    DROP CONSTRAINT name_length,
    ADD CONSTRAINT name_length CHECK (char_length(name) > 0 AND char_length(name) < 128);