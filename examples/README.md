

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


# full transformation


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
