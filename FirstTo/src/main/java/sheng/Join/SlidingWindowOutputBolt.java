package sheng.Join;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;


/**
 * Created by shezhao on 9/3/14.
 */
public class SlidingWindowOutputBolt extends BaseBasicBolt {
  private List<String> _bufferList;
  private int _interval;
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
    _bufferList.add(input.getStringByField("tag"));
    long current = System.currentTimeMillis() / 1000;
    if (current % _interval == 0) {
      /*
      Every x seconds
       */
      collector.emit(new Values(new Date(current * 1000).toString(), new LinkedList<String>(_bufferList)));
      _bufferList.clear();
      Utils.sleep(1000);
    }
  }

  /**
   * Declare the output schema for all the streams of this topology.
   *

   * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("TS", "tagList"));
  }

  public SlidingWindowOutputBolt(int interval) {
    _bufferList = new LinkedList<String>();
    _interval = interval;
  }
}
