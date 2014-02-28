/* =======================================================
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 * ======================================================= */

CREATE OR REPLACE FUNCTION notify_with_json() RETURNS trigger AS $$
DECLARE
  newDataJson TEXT;
  oldDataJson TEXT;
BEGIN
  IF (TG_OP = 'DELETE') THEN
  	oldDataJson := row_to_json(OLD, true);
  	newDataJson := '{}';
  ELSIF (TG_OP = 'INSERT') THEN
  	oldDataJson := '{}';
  	newDataJson := row_to_json(NEW, true);
  ELSIF (TG_OP = 'UPDATE') THEN
  	oldDataJson := row_to_json(OLD, true);
    newDataJson := row_to_json(NEW, true);
  END IF;

  PERFORM pg_notify(TG_ARGV[0],
    '{' || '"eventType":' || '"' || TG_OP || '",' ||
           '"tableName":' || '"' || TG_TABLE_NAME || '",' || 
           '"newEntity":' || newDataJson || ',' ||
           '"oldEntity":' || oldDataJson || '}');
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
