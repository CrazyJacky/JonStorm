package sheng;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by shezhao on 8/25/14.
 */
public class PrintBolt extends BaseBasicBolt {
  private Logger _logger = LoggerFactory.getLogger(PrintBolt.class);
  public void execute(Tuple input, BasicOutputCollector collector) {
    _logger.warn(input.toString());
//    collector.emit(new Values(input.toString()));
  }

  /**
   * Declare the output schema for all the streams of this topology.
   *

   * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
