package sheng;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.Broker;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.GlobalPartitionInformation;


/**
 * Created by shezhao on 8/25/14.
 */
public class FirstTopology {
//  private static Logger _logger = LoggerFactory.getLogger(FirstTopology.class);

  public SpoutConfig buildKafkaSpoutConfig() {
//    ZkHosts zkHosts = new ZkHosts("ec2-54-191-68-82.us-west-2.compute.amazonaws.com");
    GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();       // Using static host to prevent refreshing message from Zookeeper manager  WFT?!
    globalPartitionInformation.addPartition(0, new Broker("ec2-54-191-68-82.us-west-2.compute.amazonaws.com", 9092));
    BrokerHosts brokerHosts = new StaticHosts(globalPartitionInformation);
    SpoutConfig config = new SpoutConfig(brokerHosts, "firstT", "", "0");
    config.scheme = new SchemeAsMultiScheme(new StringScheme());
    return config;
  }

  public StormTopology build() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("in", new KafkaSpout(buildKafkaSpoutConfig()));
    builder.setBolt("out", new PrintBolt()).shuffleGrouping("in");                 // Printing every messages to worker's log file
    builder.setBolt("count", new CountSumPBolt()).shuffleGrouping("in");           // Counting messages and print every 5 messages
    return builder.createTopology();
  }

  public static void main(String[] args)
      throws AlreadyAliveException, InvalidTopologyException {
    FirstTopology firstTopology = new FirstTopology();
    Config config = new Config();
    config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
    StormTopology build = firstTopology.build();
    StormSubmitter.submitTopology("Hello", config, build);
  }
}
