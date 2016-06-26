**Install**

```
git clone https://connect.netangels.net/pastor/oddeye-storm.git
cd oddete-storm
mvn install
```

**Deploy**

{STORM_HOME}bin/storm jar projectjarfile mainclass projectconfig.file
projectjarfile-> KafkaHbaseStorm-0.2.jar mainclass -> com.oddeye.storm.KakaHbaseTopology

```
cd target
/usr/local/storm/bin/storm jar KafkaHbaseStorm-0.2.jar com.oddeye.storm.KafkaHbaseTopology ../config.yaml
```

