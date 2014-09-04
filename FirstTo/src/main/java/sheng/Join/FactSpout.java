package sheng.Join;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;


/**
 * Created by shezhao on 9/3/14.
 */
public class FactSpout extends BaseRichSpout {
  private Fields _outputFields;
  private SpoutOutputCollector _collector;
  /**
   * Declare the output schema for all the streams of this topology.
   *

   * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(_outputFields);
  }

  /**
   * Called when a task for this component is initialized within a worker on the cluster.
   * It provides the spout with the environment in which the spout executes.
   *
   * <p>This includes the:</p>
   *  @param conf The Storm configuration for this spout. This is the configuration provided to the topology merged in with cluster configuration on this machine.
   * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.

   * @param collector The collector is used to emit tuples from this spout. Tuples can be emitted at any time, including the open and close methods. The collector is thread-safe and should be saved as an instance variable of this spout object.
   */
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  /**
   * When this method is called, Storm is requesting that the Spout emit tuples to the
   * output collector. This method should be non-blocking, so if the Spout has no tuples
   * to emit, this method should return. nextTuple, ack, and fail are all called in a tight
   * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous
   * to have nextTuple sleep for a short amount of time (like a single millisecond)
   * so as not to waste too much CPU.
   */
  @Override
  public void nextTuple() {
    Utils.sleep(1000);
    _collector.emit(new Values("a"));
    _collector.emit(new Values("b"));
    _collector.emit(new Values("b"));
    _collector.emit(new Values("c"));
    _collector.emit(new Values("c"));
    _collector.emit(new Values("c"));
    _collector.emit(new Values("d"));
    _collector.emit(new Values("d"));
    _collector.emit(new Values("d"));
    _collector.emit(new Values("d"));
  }

  public FactSpout() {
    _outputFields = new Fields("tag");
  }
}
