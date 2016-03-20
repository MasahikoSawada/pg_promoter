pg_promoter
===========

pg_promoter is simplified clustring module for PostgreSQL.

# Overview
pg_promoter module works fine only on standby server side.
pg_promoter continues polling to primary server every
pg_promoter.keepalive_time second using simple query 'SELECT 1'.
If pg_promoter failes to poll pg_promoter.keepalive_count time(s),
pg_promoter will promote standby server to master server, and then
exit its process.
That is, fail over time can be calculated with this formula.

F/O time = pg_promoter.keepalives_time * pg_promoter.keepalives_count

# Paramters
- pg_promoter.primary_conninfo
Specifies a connection string to be used for pg_promoter to connect to master server.
This value must be specified in postgresql.conf, and must be same as primary_conninfo in recovery.conf.

- pg_promoter.keepalive_time (sec)
Specifies how long interval pg_promoter continues polling.
Deafult value is 5 secound.

- pg_promoter.keepalive_count
Specifies how many times pg_promoter try polling to master server in ordre to promote
standby server.
Default value is 1 times.

- pg_promoter.trigger_file
Specifies a tigger file to be used for pg_promoter to promote standby server.
This value must be same as trigger_file in postgresql.conf.
Default value is 'promote'.

# How to install pg_promoter

```
$ cd pg_promoter
$ make USE_PGXS=1
$ su
# make install
```

# How to set up pg_promoter

```
$ vi postgresql.conf
shared_preload_libraries = 'pg_promoter'
pg_promoter.keepalive_time = 5
pg_promoter.keepalive_count = 3
pg_promoter.primary_conninfo = 'host=192.168.100.100 port=5432 dbname=postgres'
```