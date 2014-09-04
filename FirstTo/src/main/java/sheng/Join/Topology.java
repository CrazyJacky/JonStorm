package sheng.Join;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import sheng.PrintBolt;


/**
 * Created by shezhao on 9/3/14.
 */
public class Topology {

  public StormTopology run() {
    Fields tagField = new Fields("tag");
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("fact", new FactSpout());
    topologyBuilder.setSpout("lkp", new LkpSpout());
    topologyBuilder.setBolt("join", new JoinBolt()).fieldsGrouping("fact", tagField).fieldsGrouping("lkp", tagField);
    topologyBuilder.setBolt("buffer", new SlidingWindowOutputBolt(5)).shuffleGrouping("join");
    topologyBuilder.setBolt("aggregate", new AggregateBolt()).shuffleGrouping("buffer");
    topologyBuilder.setBolt("print", new PrintBolt()).shuffleGrouping("aggregate");

    return topologyBuilder.createTopology();
  }

  public static void main(String[] args) {
    Topology topology = new Topology();
    Config config = new Config();
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("lo", config, topology.run());
  }
}
