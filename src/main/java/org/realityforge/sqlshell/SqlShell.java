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
import java.util.logging.Level;
import java.util.logging.Logger;

public class SqlShell
{
  private Logger _logger;
  private Driver _driver;
  private String _database;
  private final Properties _dbProperties = new Properties();

  public Logger getLogger()
  {
    return _logger;
  }

  public void setLogger( final Logger logger )
  {
    _logger = logger;
  }

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

  public List<Map<String, Object>> query( final String sql )
    throws Exception
  {
    final Connection connection = _driver.connect( _database, _dbProperties );

    final Statement statement = connection.createStatement();
    final ResultSet resultSet = statement.executeQuery( sql );

    final List<Map<String, Object>> results = toList( resultSet );

    connection.close();

    return results;
  }


  interface RowHandler
  {
    void handle( Map<String, Object> row );
  }

  private void each( final ResultSet resultSet, final RowHandler handler )
    throws Exception
  {
    for ( final Map<String, Object> row : toList( resultSet ) )
    {
      handler.handle( row );
    }
  }

  interface MapHandler<T>
  {
    T handle( Map<String, Object> row );
  }

  private <T> List<T> map( final ResultSet resultSet, final MapHandler<T> handler )
    throws Exception
  {
    final ArrayList<T> results = new ArrayList<T>();
    each( resultSet, new RowHandler()
    {
      @Override
      public void handle( final Map<String, Object> row )
      {
        results.add( handler.handle( row ) );
      }
    } );
    return results;
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
}
