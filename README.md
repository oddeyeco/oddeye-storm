For Build
  mvn install

For Deploy to storm
  {STORM_HOME}bin/storm jar  projectjarfile mainclass
  
  projectjarfile-> KafkaHbaseStorm-0.2.jar
  mainclass -> com.oddeye.storm.KakaHbaseTopology
  
  {STORM_HOME}bin/storm jar  ~/NetBeansProjects/oddeye/oddeye-storm/target/KafkaHbaseStorm-0.2.jar com.oddeye.storm.KakaHbaseTopology
