package sheng.Trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;


/**
 * Created by shezhao on 8/26/14.
 */
public class TransactionalTopology {

  public TransactionalTridentKafkaSpout kafkaSpoutBuilder(String topic) {
    BrokerHosts zkHost = new ZkHosts("ec2-54-191-125-129.us-west-2.compute.amazonaws.com");
    TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zkHost, topic);
    TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
    kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    return kafkaSpout;
  }

  public StormTopology buildTopology(String topic) {
    TridentTopology builder = new TridentTopology();
    builder.newStream("in", kafkaSpoutBuilder(topic))
        .aggregate(new Count(), new Fields("TotalCnt"))
        .each(new Fields("TotalCnt"), new ZeroFilter("TotalCnt"))
        .each(new Fields("TotalCnt"), new PrintSumFunction("TotalCnt"), new Fields("Cnt"))
    ;

    return builder.build();
  }

  public static void main(String[] args)
      throws AlreadyAliveException, InvalidTopologyException {
    TransactionalTopology topology = new TransactionalTopology();
    Config stormConfig = new Config();
    StormTopology stormTopology = topology.buildTopology("joowoo");
    LocalCluster localCluster = new LocalCluster();
    StormSubmitter.submitTopology("Hello", stormConfig, stormTopology);
  }

}
