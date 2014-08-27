package sheng.Trident;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;


/**
 * Created by shezhao on 8/26/14.
 */
public class ZeroFilter extends BaseFilter {
  @Override
  public boolean isKeep(TridentTuple tuple) {
    return (tuple.getLongByField(_fieldName) != null && tuple.getLongByField(_fieldName) != 0);
  }

  public ZeroFilter(String fieldName) {
    _fieldName = fieldName;
  }

  private String _fieldName;
}
