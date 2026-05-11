The following explains in low level detail how Postgres replicates changes from a primary and a replica server. These changes were reverse engineering by sniffing Postgres wire protocol replication traffic using [pgroxy](https://github.com/imor/pgroxy). The full trace from which this information has been collected is in the replication_trace.txt file in this folder.

Connection 1
============

1. Connect with following params:
    Parameter: database = pub
    Parameter: replication = database
    Parameter: application_name = mysub

2. Run query: SELECT t.pubname FROM
              pg_catalog.pg_publication t WHERE
              t.pubname IN ('mypub')
    Result:
        pubname = "mypub"

3. Run query: SELECT DISTINCT t.schemaname, t.tablename
              , t.attnames
              FROM pg_catalog.pg_publication_tables t
              WHERE t.pubname IN ('mypub')
    Result:
        schemaname = "public",
        tablename = "table_1",
        attnames = "{id,name}"

4. Run query: CREATE_REPLICATION_SLOT "mysub" LOGICAL pgoutput (SNAPSHOT 'nothing')
    Result:
        slotname = "mysub",
        consistent_point = "0/19BD9E8",
        snapshot_name = "",
        output_plugin = "pgoutput"

5. Close connection


Connection 2 (CDC connection)
=============================

1. Connect with following params:
    Parameter: database = pub
    Parameter: replication = database
    Parameter: application_name = mysub

2. Run query: IDENTIFY_SYSTEM
    Result:
        systemid = "7329763972895242536"
        timelin = "1"
        xlogpos = "0/19BD9E8"
        dbname = "pub"

3. Run query: START_REPLICATION SLOT "mysub" LOGICAL 0/0 (proto_version '3', publication_names '"mypub"')
    Result:
        server sends two keep alive messages with "client reply" bit set to 0
        client sends standby status update: Data:
            received_lsn: 0, 0, 0, 0, 1, 155, 217, 232,
            flushed_lsn:  0, 0, 0, 0, 1, 155, 217, 232,
            applied_lsn:  0, 0, 0, 0, 1, 155, 217, 232,
            client_time: 0, 2, 179, 42, 70, 55, 57, 124
            ping: 0


Connection 3 (snapshot connection)
==================================

1. Connect with following params:
    Parameter: database = pub
    Parameter: replication = database
    Parameter: application_name = pg_16406_sync_16399_7329764006882527550

2. Run query: BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ
    Result:

3. Run query: CREATE_REPLICATION_SLOT "pg_16406_sync_16399_7329764006882527550" LOGICAL pgoutput (SNAPSHOT 'use')
    Result:
        Column: slot_name = "pg_16406_sync_16399_7329764006882527550"
        Column: consistent_point = "0/19BDA20"
        Column: snapshot_name = ""
        Column: output_plugin = "pgoutput"

4. Run query: SELECT c.oid, c.relreplident, c.relkind  FROM pg_catalog.pg_class c  INNER JOIN pg_catalog.pg_namespace n        ON (c.relnamespace = n.oid) WHERE n.nspname = 'public'   AND c.relname = 'table_1'
    Result:
        Column: oid = "16389"
        Column: relreplident = "d"
        Column: relkind = "r"

5. Run query: SELECT DISTINCT  (CASE WHEN (array_length(gpt.attrs, 1) = c.relnatts)   THEN NULL ELSE gpt.attrs END)  FROM pg_publication p,  LATERAL pg_get_publication_tables(p.pubname) gpt,  pg_class c WHERE gpt.relid = 16389 AND c.oid = gpt.relid   AND p.pubname IN ( 'mypub' )
    Result:
        Column: attrs = ""

6. Run query: SELECT a.attnum,       a.attname,       a.atttypid,       a.attnum = ANY(i.indkey)  FROM pg_catalog.pg_attribute a  LEFT JOIN pg_catalog.pg_index i       ON (i.indexrelid = pg_get_replica_identity_index(16389)) WHERE a.attnum > 0::pg_catalog.int2   AND NOT a.attisdropped AND a.attgenerated = ''   AND a.attrelid = 16389 ORDER BY a.attnum
    Result:
        Column: attnum = "1"
        Column: attname = "id"
        Column: atttypid = "23"
        Column: ?column? = "t"

        Column: attnum = "2"
        Column: attname = "name"
        Column: atttypid = "1043"
        Column: ?column? = "f"

6. Run query: SELECT DISTINCT pg_get_expr(gpt.qual, gpt.relid)  FROM pg_publication p,  LATERAL pg_get_publication_tables(p.pubname) gpt WHERE gpt.relid = 16389   AND p.pubname IN ( 'mypub' )
    Result:
        Column: pg_get_expr = ""

7. Run query: COPY public.table_1 (id, name) TO STDOUT
    Result:
        Data: [49, 9, 100, 97, 116, 97, 49, 10]
        Data: [50, 9, 100, 97, 116, 97, 50, 10]
        Data: [51, 9, 100, 97, 116, 97, 51, 10]
        Data: [52, 9, 100, 97, 116, 97, 52, 10]
        Data: [53, 9, 100, 97, 116, 97, 53, 10]
        Data: [54, 9, 100, 97, 116, 97, 54, 10]
        Data: [55, 9, 100, 97, 116, 97, 55, 10]
        Data: [56, 9, 100, 97, 116, 97, 56, 10]
        Data: [57, 9, 100, 97, 116, 97, 57, 10]
        Data: [49, 48, 9, 100, 97, 116, 97, 49, 48, 10]

8. Run query: COMMIT

9. Run query: START_REPLICATION SLOT "pg_16406_sync_16399_7329764006882527550" LOGICAL 0/19BDA20 (proto_version '3', publication_names '"mypub"')
    Result:
        ← Data: [107, 0, 0, 0, 0, 1, 155, 218, 32, 0, 2, 179, 42, 70, 55, 146, 232, 0]
        → Data: [114, 0, 0, 0, 0, 1, 155, 218, 32, 0, 0, 0, 0, 1, 155, 218, 32, 0, 0, 0, 0, 1, 155, 218, 32, 0, 2, 179, 42, 70, 55, 146, 255, 0]

10. Run query: DROP_REPLICATION_SLOT pg_16406_sync_16399_7329764006882527550 WAIT
    Result:

11. Terminate
