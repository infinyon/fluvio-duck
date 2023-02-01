# Introduction

fluvio-duck is a DuckDB extension to work with Fluvio streaming platform.  It allows you to query Fluvio topics and partitions using SQL.  It also allows you to consume Fluvio topics using SQL.

# Building extension

For debugging and testing
## Debug 

Execute the following command to build the extension in debug mode:
```
make debug
```

Loading extension:

```
$ ./build/debug/duckdb --unsigned

D load './build/debug/extension/fluvio-duck/fluvioduck.duckdb_extension';
```

Note that Debug mode should not be used for large data sets. It is very slow.

## Release

Execute the following command to build the extension in release mode.  Note that building release will take longer than debug mode.

```
make release
```

Loading extension:

```
$ ./build/release/duckdb --unsigned
D load './build/release/extension/fluvio-duck/fluvioduck.duckdb_extension';
```

# Running extension

Load either debug or release version of the extension as in the previous section.


## Getting topics and partitions

```
D select * from fluvio_topic();
┌───────────┬────────────┐
│   name    │ partitions │
│  varchar  │   int32    │
├───────────┼────────────┤
│ helsinki  │          1 │
│ cat-facts │          1 │
└───────────┴────────────┘
```

Partitions
```
select * from fluvio_partition();
┌───────────┬───────────┬─────────┐
│   topic   │ partition │   LEO   │
│  varchar  │  varchar  │  int32  │
├───────────┼───────────┼─────────┤
│ cat-facts │ 0         │   30920 │
│ helsinki  │ 0         │ 8098386 │
└───────────┴───────────┴─────────┘

```

# Helsinki Demo

This demo requires development release of CDK and MQTT connector.

## Connect to Fluvio

Either set up local Fluvio cluster (https://www.fluvio.io) or connect to Infinyon cloud at https://infinyon.cloud/signup.

Test by running this command:

```
$ fluvio topic list
< show topic list>
```

## Download Connector Developer Kit (CDK)

Download CDK by running this command:

```
$ fluvio install --hub cdk
```





Computing total number of offets
```
select sum(leo) from fluvio_partition();
┌──────────┐
│ sum(leo) │
│  int128  │
├──────────┤
│  8129308 │
└──────────┘
```

# MQTT example

## Simple select

get last 1000 events
```
 select *  from  fluvio_consume('helsinki --tail 1000');
```

## Do transformation with jolt
```
D select *  from  fluvio_consume('helsinki --tail 1000 --transforms-file=jolt.yaml');
```

view with first 10

```
D create view transit as select *  from  fluvio_consume('helsinki --tail 10 --transforms-file=jolt.yaml
-c lat:d="lat" -c long:d="long" -c vehicle:i="vehicle"  -c route="route" -c speed:d="speed" 
 -c time:t="tst"  -c acc:d="acc" -c line:i="line" -c stop:i="stop" -c desi="desi" -c operator:i="oper"
 -c dl:i="dl" -c odo:i="odo" -c drst:i="drst" -c occu:i="occu" -c hdg:i="hdg" -c dir="dir" -c tsi:i="tsi"
 -c jrn:i="jrn" -c start="start"');
```

All data
```
D create view transit as select *  from  fluvio_consume('helsinki -B --rows=1859058 --transforms-file=jolt.yaml
-c lat:d="lat" -c long:d="long" -c vehicle:i="vehicle"  -c route="route" -c speed:d="speed" 
 -c time:t="tst"  -c acc:d="acc" -c line:i="line" -c stop:i="stop" -c desi="desi" -c operator:i="oper"
 -c dl:i="dl" -c odo:i="odo" -c drst:i="drst" -c occu:i="occu" -c hdg:i="hdg" -c dir="dir" -c tsi:i="tsi"
 -c jrn:i="jrn" -c start="start"');
```


```
D select avg(speed) from events where route == '3001R';
```

## Convert to Parquet

```
D INSTALL parquet; Load 'parquet';

D COPY (SELECT * FROM transit) TO 'helsinki.parquet' (FORMAT 'parquet');
D create view pevents as SELECT * FROM read_parquet('helsinki.parquet');
```

Performance

```
EXPLAIN analyze  select avg(speed) from pevents;
```


