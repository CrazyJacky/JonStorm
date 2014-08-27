package sheng.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * Created by shezhao on 8/26/14.
 */
public abstract class DBBase {
  protected Connection _connection;

  protected Connection getConnection(String jdbcStr)
      throws SQLException {
    _connection = DriverManager.getConnection(jdbcStr);
    return _connection;
  }

  protected void closeConnection()
      throws SQLException {
    _connection.close();
  }
}

