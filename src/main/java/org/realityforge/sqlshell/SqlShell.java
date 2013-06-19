package org.realityforge.sqlshell;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class SqlShell
{
  private Driver _driver;
  private String _database;
  private final Properties _dbProperties = new Properties();

  public Driver getDriver()
  {
    return _driver;
  }

  public void setDriver( final Driver driver )
  {
    _driver = driver;
  }

  public String getDatabase()
  {
    return _database;
  }

  public void setDatabase( final String database )
  {
    _database = database;
  }

  public Properties getDbProperties()
  {
    return _dbProperties;
  }

  public Result execute( final String sql )
    throws Exception
  {
    final Connection connection = getConnection();
    try
    {
      final Statement statement = connection.createStatement();
      final boolean returnsResultSet = statement.execute( sql );
      if ( returnsResultSet )
      {
        return new Result( toList( statement.getResultSet() ), 0 );
      }
      else
      {
        return new Result( null, statement.getUpdateCount() );
      }
    }
    finally
    {
      connection.close();
    }
  }

  public List<Map<String, Object>> query( final String sql )
    throws Exception
  {
    final Connection connection = getConnection();
    try
    {
      final Statement statement = connection.createStatement();
      final boolean returnsResultSet = statement.execute( sql );
      if ( returnsResultSet )
      {
        return toList( statement.getResultSet() );
      }
      else
      {
        return new ArrayList<Map<String, Object>>();
      }
    }
    finally
    {
      connection.close();
    }
  }

  public int executeUpdate( final String sql )
    throws Exception
  {
    final Connection connection = getConnection();
    try
    {
      return connection.createStatement().executeUpdate( sql );
    }
    finally
    {
      connection.close();
    }
  }

  private List<Map<String, Object>> toList( final ResultSet resultSet )
    throws SQLException
  {
    final ResultSetMetaData md = resultSet.getMetaData();
    final int columns = md.getColumnCount();
    final ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    while ( resultSet.next() )
    {
      final HashMap<String, Object> row = new HashMap<String, Object>();
      for ( int i = 1; i <= columns; ++i )
      {
        row.put( md.getColumnName( i ), resultSet.getObject( i ) );
      }
      list.add( row );
    }

    return list;
  }

  private Connection getConnection()
    throws SQLException
  {
    final Connection connection = _driver.connect( _database, _dbProperties );
    if( null == connection )
    {
      throw new IllegalStateException( "Driver does not match jdbc url" );
    }
    return connection;
  }
}
