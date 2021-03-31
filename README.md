**Install**

```
git clone https://github.com/oddeyeco/oddeye-storm.git
cd oddete-storm
mvn clean package
```

**Deploy**

{STORM_HOME}bin/storm jar projectjarfile mainclass projectconfig.file
projectjarfile-> KafkaHbaseStorm-0.2.jar mainclass -> co.oddeye.storm.KakaHbaseTopology

```
cd target
/srv/storm/bin/storm jar TimeSeriesTopology-0.15.0.jar  co.oddeye.storm.TimeSeriesTopology ../config.yaml
```

config.yaml

```
Kafka:
   zkHosts: "zoo1.domain.com:2181,zoo2.domain.com:2181,zoo3.domain.com:2181"       
   tsdbtopic: "oddeyetsdb"   
   zkRoot: "/storm_oddeye"
   zkKey: "oddeye-staus"
   zkKeyTSDB: "oddeye-TSDBstat"
   
ErrorKafka:
    bootstrap.servers: "kafka1.domain.com:9092,kafka2.domain.com:9092,kafka3.domain.com:9092" 
    topic: "errors"

KafkaSemaphore:
   zkHosts: "zoo1.domain.com:2181,zoo2.domain.com:2181,zoo3.domain.com:218"       
   topic: "semaphore"   
   zkRoot: "/semaphore_oddeye"
   zkKey: "semaphore-staus"       
   
Tsdb:
  zkHosts: "zoo1.domain.com:2181,zoo2.domain.com:2181,zoo3.domain.com:218"
  hbase.rpcs.batch.size: 4096       
  metatable: oddeye-meta
  errorstable: oddeye-errors
  errorshistorytable: oddeye-error-history
  errorslasttable: oddeye-error-last
  usertable: oddeyeusers  
  consumptionusertable: oddeye-consumption
  optionstable: oddeyeOptionstable    
  messageprice: 9.6450617283950617283950617283950617283950617283950617e-7

 
  tsd.core.auto_create_metrics: true
  tsd.storage.enable_compaction: false
  tsd.storage.hbase.data_table: oddeye-data
  tsd.storage.hbase.uid_table: oddeye-data-uid

   
mail:
    from: noreply@domain.com
    smtp.host: mail.domain.com
    smtp.auth: true
    smtp.port: 25
    mail.user: noreply@domain.com
    mail.password: XXxxXXxx
    

Topology:    
    topologi.display.name: "OddeyeTimeSeriesTopology"
    NumWorkers: 3
    SpoutParallelism_hint: 3
    SpoutSemaphoreParallelism_hint: 1
    ParseMetricBoltParallelism_hint: 6
    SemaforProxyBoltParallelism_hint: 2
    WriteToTSDBseriesParallelism_hint: 9
    CompareBoltParallelism_hint: 12
    CalcRulesBoltParallelism_hint: 6
    ErrorKafkaHandlerParallelism_hint: 12
    CheckSpecialErrorBoltParallelism_hint: 9
    MetricErrorToHbaseParallelism_hint: 12
    SendNotifierBoltParallelism_hint: 12
    UserBalaceCalcBoltParallelism_hint: 6
    Debug: false
    DisableCheck: false
    topology.message.timeout.secs: 30
    topology.max.spout.pending: 1024

```
