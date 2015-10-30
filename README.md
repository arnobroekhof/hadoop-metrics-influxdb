# hadoop-metrics-influxdb
Hadoop metrics sink for influxdb

This is the first version

## Configuration hadoop

*.sink.file.class=org.apache.hadoop.metrics2.sink.InfluxdbSink
*.sink.influxdb.influxdb_host=influxdb.example.com
*.sink.influxdb.influxdb_port=8086
*.sink.influxdb.influxdb_database=hadoop
*.sink.influxdb.influxdb_username=root
*.sink.influxdb.influxdb_password=root
*.sink.ingluxdb.cluster=clustername
