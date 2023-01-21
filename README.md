#Loading

## Debug 

```
make debug
```

Loading extension:

```
./build/debug/duckdb --unsigned

load './build/debug/extension/fluvio-duck/fluvioduck.duckdb_extension';
>> getting version
>> init
```

## Release

```
make release
```

Loading extension:

```
$ ./build/release/duckdb --unsigned
D load './build/release/extension/fluvio-duck/fluvioduck.duckdb_extension';
```

# Running cloud demo
  
Start cats-facts and helinki in the cloud demo repo.
Download Jolt and Regex SmartModule in the UI console.


# Getting topics and partitions

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


