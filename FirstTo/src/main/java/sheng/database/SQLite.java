package sheng.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by shezhao on 8/26/14.
 */
public class SQLite extends DBBase {

  public SQLite(String jdbc)
      throws SQLException, ClassNotFoundException {
    Class.forName("org.sqlite.JDBC");
    getConnection(jdbc);
  }

  public void createTable(String table)
      throws SQLException {
    Statement statement = _connection.createStatement();
    String sql = "CREATE TABLE " + table +
        "(CNT DECIMAL(18,0))";
    statement.executeUpdate(sql);
    sql = "insert into " + table + " values(0)";
    statement.executeQuery(sql);
  }

  public void setNum(String table, long num)
      throws SQLException {
    String prepared = "update " + table + " set CNT = ?";
    PreparedStatement preparedStatement = _connection.prepareStatement(prepared);
    preparedStatement.setLong(1, num);
    preparedStatement.executeUpdate();
  }


  public boolean isTableExists(String table) {
    try {
      String p = "select count(*) from " + table;
      PreparedStatement preparedStatement = _connection.prepareStatement(p);
      preparedStatement.executeQuery();
    } catch (SQLException e) {
      return false;
    }
    return true;
  }

  public void close()
      throws SQLException {
    closeConnection();
  }
}
