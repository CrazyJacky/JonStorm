package sheng;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


/**
 * Created by shezhao on 8/25/14.
 */
public class FirstTopology {
//  private static Logger _logger = LoggerFactory.getLogger(FirstTopology.class);

  public SpoutConfig buildKafkaSpoutConfig(String topic) {
    ZkHosts zkHosts = new ZkHosts("ec2-54-191-125-129.us-west-2.compute.amazonaws.com");
    SpoutConfig config = new SpoutConfig(zkHosts, topic, "", "0");
    config.scheme = new SchemeAsMultiScheme(new StringScheme());
    return config;
  }

  public StormTopology build(String topic) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("in", new KafkaSpout(buildKafkaSpoutConfig(topic)));
    builder.setBolt("out", new PrintBolt()).shuffleGrouping("in");                 // Printing every messages to worker's log file
    builder.setBolt("count", new CountSumPBolt()).shuffleGrouping("in");           // Counting messages and print every 5 messages
    return builder.createTopology();
  }

  public static void main(String[] args)
      throws AlreadyAliveException, InvalidTopologyException {
    FirstTopology firstTopology = new FirstTopology();
    Config config = new Config();
//    config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
    StormTopology build = firstTopology.build(args[0]);
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("Hello JooWoo", config, build);
  }
}
