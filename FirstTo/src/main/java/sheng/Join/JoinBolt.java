package sheng.Join;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by shezhao on 9/3/14.
 */
public class JoinBolt extends BaseBasicBolt {
  private Fields _tagFields;
  private Fields _outputFields;
  private Set<String> _lkpMap;

  /**
   * Process the input tuple and optionally emit new tuples based on the input tuple.
   *
   * All acking is managed for you. Throw a FailedException if you want to fail the tuple.

   * @param
  input
   * @param collector
   */
  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    if (input.getSourceComponent().equals("lkp")) {
      if (! _lkpMap.contains(input.getStringByField("tag"))) {
        _lkpMap.add(input.getStringByField("tag"));
      }
    } else {
      String val = input.getStringByField("tag");
      if (_lkpMap.contains(val)) {
        collector.emit(new Values(val));
      }
    }

  }

  /**
   * Declare the output schema for all the streams of this topology.
   *

   * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(_outputFields);
  }

  public JoinBolt() {
    _outputFields = new Fields("tag");
    _lkpMap = new HashSet<String>();
    _tagFields = new Fields("tag");
  }
}
