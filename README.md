**Install**

```
git clone https://connect.netangels.net/pastor/oddeye-storm.git
cd oddete-storm
mvn install
```

**Deploy**

{STORM_HOME}bin/storm jar projectjarfile mainclass projectconfig.file
projectjarfile-> KafkaHbaseStorm-0.2.jar mainclass -> co.oddeye.storm.KakaHbaseTopology

```
cd target
/usr/local/storm/bin/storm jar KafkaHbaseStorm-0.2.jar co.oddeye.storm.KafkaHbaseTopology ../config.yaml
```

