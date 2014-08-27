package sheng.Trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * Created by shezhao on 8/26/14.
 */
public class PrintSumFunction extends BaseFunction {
  private static Logger _logger = LoggerFactory.getLogger(PrintSumFunction.class);
  private long _sum;
  private String _sumField;

  public void execute(TridentTuple tuple, TridentCollector collector) {
    if (tuple.size() == 0) {
      return;
    }
    _sum += tuple.getLongByField(_sumField);
    if (_sum % 8 == 0) {
      _logger.info("Sum Accumulating: " + _sum);
    }
  }

  public PrintSumFunction(String sumField) {
    _sumField = sumField;
  }
}
