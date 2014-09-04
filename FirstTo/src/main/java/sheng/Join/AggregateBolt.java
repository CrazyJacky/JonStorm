package sheng.Join;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.List;


/**
 * Created by shezhao on 9/3/14.
 */
public class AggregateBolt extends BaseBasicBolt {
  private HashMap<String, Integer> _aggredResult;
  /**
   * Process the input tuple and optionally emit new tuples based on the input tuple.
   *
   * All acking is managed for you. Throw a FailedException if you want to fail the tuple.

   * @param
  input
   * @param collector
   */
  @Override
  @SuppressWarnings("unchecked")
  public void execute(Tuple input, BasicOutputCollector collector) {
    _aggredResult.clear();
    String ts = input.getStringByField("TS");
    List<String> tags = (List<String>) input.getValueByField("tagList");
    for (String tag : tags) {
      if (!_aggredResult.containsKey(tag)) {
        _aggredResult.put(tag, 1);
      } else {
        int newVal = _aggredResult.get(tag) + 1;
        _aggredResult.put(tag, newVal);
      }
    }
    for (String s : _aggredResult.keySet()) {
      collector.emit(new Values(ts, s, _aggredResult.get(s)));
    }
  }

  /**
   * Declare the output schema for all the streams of this topology.
   *

   * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("timeStamp", "tag", "Cnt"));
  }

  public AggregateBolt() {
    _aggredResult = new HashMap<String, Integer>();
  }
}
