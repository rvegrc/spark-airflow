-- In ClickHouse, the TRUNCATE privilege doesn't exist as a standalone privilege. 
-- Instead, you should use the appropriate privileges that allow truncating tables.
GRANT SELECT, INSERT, CREATE, DROP, ALTER ON dev_transerv_cont.* TO role developer;