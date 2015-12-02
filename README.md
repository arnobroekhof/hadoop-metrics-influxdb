# hadoop-metrics-influxdb
Hadoop metrics sink for influxdb 

tested on Hadoop version 2.7.1

## Configuration hadoop
file: hadoop-metrics2.properties

```
*.sink.file.class=org.apache.hadoop.metrics2.sink.InfluxdbSink
*.sink.influxdb.influxdb_host=influxdb.example.com
*.sink.influxdb.influxdb_port=8086
*.sink.influxdb.influxdb_database=hadoop
*.sink.influxdb.influxdb_username=hadoop
*.sink.influxdb.influxdb_password=hadoop
*.sink.ingluxdb.cluster=clustername
```
