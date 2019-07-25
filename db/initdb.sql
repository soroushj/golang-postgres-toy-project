-- id sequence and functions

CREATE SEQUENCE data_items_id_seq;

CREATE OR REPLACE FUNCTION new_id() RETURNS bigint AS $$
BEGIN
  RETURN ((extract(epoch from clock_timestamp()) * 1000000)::bigint << 12) | (nextval('data_items_id_seq') % 4096);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION timestamp_from_id(id bigint) RETURNS timestamptz AS $$
BEGIN
  RETURN to_timestamp((id >> 12) / 1000000.0);
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;


-- data_items table

CREATE TABLE data_items (
  id bigint PRIMARY KEY DEFAULT new_id(),
  namespace text NOT NULL,
  key text NOT NULL,
  version bigint NOT NULL,
  content jsonb NOT NULL,
  CONSTRAINT data_items_nkv_unique UNIQUE (namespace, key, version)
);

CREATE INDEX data_items_nki ON data_items (namespace, key, id DESC);
CREATE INDEX data_items_nkai ON data_items (namespace, key, (content->>'author'), id DESC);


-- random data for data_items

INSERT INTO data_items (id, namespace, key, version, content)
SELECT
  ((extract(epoch from '2010-01-01 00:00:00'::timestamptz + random() * ('2018-01-01 00:00:00'::timestamptz - '2010-01-01 00:00:00'::timestamptz)) * 1000000)::bigint << 12) | floor(random() * 4096)::int AS id,
  floor(random() * 1000)::text AS namespace,
  floor(random() * 1000000000000)::text AS key,
  floor(random() * 1000000000000)::bigint AS version,
  ('{"author":"' || floor(random() * 100000)::text || '"}')::jsonb AS content
FROM generate_series(1, 1000000) s;


-- namespace_most_recent_ids materialized view

CREATE MATERIALIZED VIEW namespace_most_recent_ids AS
SELECT
  namespace,
  max(id) AS most_recent_id
FROM data_items
GROUP BY namespace;

CREATE UNIQUE INDEX namespace_most_recent_ids_i_unique ON namespace_most_recent_ids (most_recent_id DESC);


-- triggers

CREATE OR REPLACE FUNCTION set_version_tg() RETURNS trigger AS $$
DECLARE
  max_version bigint;
BEGIN
  IF NEW.version IS NULL THEN
    SELECT max(version) FROM data_items WHERE namespace = NEW.namespace AND key = NEW.key INTO max_version;
    IF max_version IS NULL THEN
      NEW.version := 0;
    ELSE
      NEW.version := max_version + 1;
    END IF;
  END IF;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_version
BEFORE INSERT
ON data_items FOR EACH ROW 
EXECUTE PROCEDURE set_version_tg();

CREATE OR REPLACE FUNCTION refresh_namespace_most_recent_ids_tg() RETURNS trigger AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY namespace_most_recent_ids;
  RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_namespace_most_recent_ids
AFTER INSERT OR UPDATE OF id, namespace OR DELETE OR TRUNCATE
ON data_items FOR EACH STATEMENT 
EXECUTE PROCEDURE refresh_namespace_most_recent_ids_tg();
