package sheng.Trident;

import java.sql.SQLException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sheng.database.SQLite;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;


/**
 * Created by shezhao on 8/26/14.
 */
public class Sum2DBFunction extends BaseFunction {
  private static Logger _logger = LoggerFactory.getLogger(Sum2DBFunction.class);
  private long _sum;
  private String _sumField;
  private String _dbName = "jooDB";
  private String _tableName = "totalMsgCnt";
  private SQLite _sqLite;

  public void execute(TridentTuple tuple, TridentCollector collector) {
    if (tuple.size() == 0) {
      return;
    }
    _sum += tuple.getLongByField(_sumField);
    if (_sum % 8 == 0) {
      _logger.info("Sum Accumulating: " + _sum);
      try {
        _sqLite.setNum(_tableName, _sum);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void prepare(Map conf, TridentOperationContext context) {
    try {
      _sqLite = new SQLite("jdbc:sqlite:/tmp/sumCount.db");
      if (! _sqLite.isTableExists(_tableName)) {
        _sqLite.createTable(_tableName);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void cleanup() {
    try {
      _sqLite.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public Sum2DBFunction(String sumField) {
    _sumField = sumField;
  }
}
