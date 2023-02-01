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

# Using fluvio-duck extension

Load either debug or release version of the extension as in the previous section.


## Getting topics and partitions

You can use the following commands to get list of topics and partitions.

To get list of topics:
```
D select * from fluvio_topics();
┌───────────┬────────────┐
│   name    │ partitions │
│  varchar  │   int32    │
├───────────┼────────────┤
│ helsinki  │          1 │
│ cat-facts │          1 │
└───────────┴────────────┘
```

To get list of partitions:
```
select * from fluvio_partitions();
┌───────────┬───────────┬─────────┐
│   topic   │ partition │   LEO   │
│  varchar  │  varchar  │  int32  │
├───────────┼───────────┼─────────┤
│ cat-facts │ 0         │   30920 │
│ helsinki  │ 0         │ 8098386 │
└───────────┴───────────┴─────────┘
```

With SQL, you can sum up all the partitions to get total number of offsets.

```
D select sum(leo) from fluvio_partitions();
┌──────────┐
│ sum(leo) │
│  int128  │
├──────────┤
│  1859060 │
└──────────┘
```

## Querying Fluvio topics

With SQL, you can query Fluvio topics and materialize as SQL table.

The command follow the format:

```sql
D select <param> from fluvio_consume('<topic_name> <options>');
```

The options are same as in the Fluvio CLI except options related to output format. 

For example, to get last 5 events from topic `helsinki`:

```sql
D select * from fluvio_consume('helsinki --tail 5');
┌─────────┬──────────────────────┬────────────────────────────────────────────────────────────────────┐
│ offset  │      timestamp       │                               value                                │
│  int32  │     timestamp_ms     │                              varchar                               │
├─────────┼──────────────────────┼────────────────────────────────────────────────────────────────────┤
│ 1859053 │ 2023-01-28 23:54:2…  │ {"mqtt_topic":"/hfp/v2/journey/ongoing/vp/bus/0018/00258/1065/1/…  │
│ 1859054 │ 2023-01-28 23:54:2…  │ {"mqtt_topic":"/hfp/v2/journey/ongoing/vp/train/0090/06065/3001T…  │
│ 1859055 │ 2023-01-28 23:54:2…  │ {"mqtt_topic":"/hfp/v2/journey/ongoing/vp/bus/0022/00971/2118N/2…  │
│ 1859056 │ 2023-01-29 00:12:5…  │ {"mqtt_topic":"/hfp/v2/journey/ongoing/vp/train/0090/06326/3001T…  │
│ 1859057 │ 2023-01-29 00:12:5…  │ {"mqtt_topic":"/hfp/v2/journey/ongoing/vp/bus/0022/01360/1085N/2…  │
└─────────┴──────────────────────┴────────────────────────────────────────────────────────────────────┘

```

You can ask for help by using `--help` option:

```sql
select * from fluvio_consume('--help');
.... help command output
```

## SmartModule Transformations

You can use SmartModule transformations to transform the data.  The transformations are defined in a YAML file.  The file is passed to the `--transforms-file` option.

For example, to get the last 1000 events from topic `helsinki` and transform the data using `jolt.yaml` file:
```
D select *  from  fluvio_consume('helsinki --tail 5 --transforms-file=examples/short.yaml');

select *  from  fluvio_consume('helsinki --tail 5 --transforms-file=examples/short.yaml');
┌─────────┬──────────────────────┬────────────────────────────────────────────────────────────────────┐
│ offset  │      timestamp       │                               value                                │
│  int32  │     timestamp_ms     │                              varchar                               │
├─────────┼──────────────────────┼────────────────────────────────────────────────────────────────────┤
│ 1859053 │ 1969-12-31 23:59:5…  │ {"acc":0.0,"desi":"65","dir":"1","dl":-19,"drst":0,"hdg":109,"jr…  │
│ 1859054 │ 1969-12-31 23:59:5…  │ {"acc":0.15,"desi":"T","dir":"1","dl":-180,"drst":null,"hdg":357…  │
│ 1859055 │ 1969-12-31 23:59:5…  │ {"acc":-0.56,"desi":"118N","dir":"2","dl":-305,"drst":0,"hdg":17…  │
│ 1859056 │ 1969-12-31 23:59:5…  │ {"acc":-0.48,"desi":"T","dir":"1","dl":3419,"drst":null,"hdg":18…  │
│ 1859057 │ 1969-12-31 23:59:5…  │ {"acc":0.0,"desi":"85N","dir":"2","dl":719,"drst":0,"hdg":null,"…  │
└─────────┴──────────────────────┴────────────────────────────────────────────────────────────────────┘

```

This assumes you have downloaded jolt SmartModule from the hub.  Please see fluvio SmartModule documentation for more information.

## Mapping JSON columns to SQL columns

In the previous example, the JSON data is returned as a single column.  You can map the JSON columns to SQL columns using the `-c` option.  The `-c` option takes a column name and a JSON path.  The JSON path is a dot separated path to the JSON column.  For example, to map the `lat` column to `d` column, you can use `-c lat:d="lat"`.   

Following example show how to create materialized view with mapped columns:

```
D create view transit as select *  from  fluvio_consume('helsinki --tail 5 --transforms-file=examples/short.yaml
-c lat:d="lat" -c long:d="long" -c vehicle:i="vehicle"  -c route="route" -c speed:d="speed" 
 -c time:t="tst"');

D select * from transit;

───────────┬───────────┬─────────┬─────────┬────────┬─────────────────────────┐
│    lat    │   long    │ vehicle │  route  │ speed  │          time           │
│  double   │  double   │  int32  │ varchar │ double │      timestamp_ms       │
├───────────┼───────────┼─────────┼─────────┼────────┼─────────────────────────┤
│ 60.170393 │ 24.944114 │     258 │ 1065    │   5.56 │ 2023-01-28 23:54:23.399 │
│ 60.174296 │ 24.941409 │    6065 │ 3001T   │   8.75 │ 2023-01-28 23:54:22.629 │
│ 60.172573 │  24.77937 │     971 │ 2118N   │   7.36 │ 2023-01-28 23:54:23.39  │
│ 60.171846 │ 24.941544 │    6326 │ 3001T   │   0.77 │ 2023-01-28 23:54:23.44  │
│ 60.170552 │ 25.079789 │    1360 │ 1085N   │    0.0 │ 2023-01-28 23:54:23.405 │
└───────────┴───────────┴─────────┴─────────┴────────┴─────────────────────────┘

```

With mapped columns, you can use SQL to perform analysis on the data.  For example, to get the average speed of the vehicles by route:

```sql
D select route, avg(speed) from transit group by route;
┌─────────┬────────────┐
│  route  │ avg(speed) │
│ varchar │   double   │
├─────────┼────────────┤
│ 1065    │       5.56 │
│ 3001T   │       4.76 │
│ 2118N   │       7.36 │
│ 1085N   │        0.0 │
└─────────┴────────────┘
```

## Converting fluvio topic data to Parquet

Previous examples show how to consume data from fluvio topic and perform SQL analysis on the data.  You can also convert the data to Parquet format and perform analysis using Parquet tools.  For example, to convert the data to Parquet format, you can use the `COPY` command:

First install Parquet extensions into DuckDB:

```
D INSTALL parquet; Load 'parquet';
```

Then run the following command to convert the data to Parquet format:

```
D COPY (SELECT * FROM <fluvio_topic>) TO '<parquet_file>' (FORMAT 'parquet');
```
For example, to convert the data from `transit` materialized view to `helsinki.parquet` file, you can run the following command:

```
D COPY (SELECT * FROM transit) TO 'helsinki.parquet' (FORMAT 'parquet');
```

Note that current version of fluvio-duck extension is not optimized for performance.  It is recommended to use the `COPY` command for small data sets.