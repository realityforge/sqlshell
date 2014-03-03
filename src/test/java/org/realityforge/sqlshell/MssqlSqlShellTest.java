package org.realityforge.sqlshell;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.sourceforge.jtds.jdbc.Driver;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MssqlSqlShellTest
{
  private static final String DB1_NAME = "sqlshell_test_db";

  @Test
  public void basicConstruction()
    throws Exception
  {
    final SqlShell shell = new SqlShell();
    final Driver driver = getDriver();
    shell.setDriver( driver );
    assertEquals( shell.getDriver(), driver );
    final String database = getDatabase();
    shell.setDatabase( database );
    assertEquals( shell.getDatabase(), database );
    copyDbPropertiesToShell( shell );

    final String schema = "x";
    final String table = "myTable";

    assertEquals( shell.executeUpdate( schema(schema) ), 0 );

    assertEquals( shell.executeUpdate( table( schema, table, column( "ID", "integer" ),
                                              column( "TS", "timestamp NOT NULL" ) ) ), 0);

    final List<Map<String, Object>> results =
      shell.query(
        "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'x' ORDER BY table_schema,table_name" );

    assertEquals( results.size(), 1 );
    assertEquals( results.get( 0 ).get( "table_schema" ), schema );
    assertEquals( results.get( 0 ).get( "table_name" ), table );
  }

  private void copyDbPropertiesToShell( final SqlShell shell )
  {
    Properties props = getDbProperties();
    Enumeration e = props.propertyNames();
    while ( e.hasMoreElements() )
    {
      String key = (String) e.nextElement();
      shell.getDbProperties().setProperty( key, props.getProperty( key ) );
    }
  }

  protected final String schema( final String schema )
  {
    return "CREATE SCHEMA \"" + schema + "\"";
  }

  protected final String table( final String schema,
                                final String table,
                                final String... elements )
  {
    return "CREATE TABLE \"" + schema + "\".\"" + table + "\"(" + join( ',', elements ) + ")";
  }

  protected final String column( final String name, final String type )
  {
    return "\"" + name + "\" " + type;
  }

  protected final String join( final char separator, final String... commands )
  {
    final StringBuilder sb = new StringBuilder();
    for ( final String command : commands )
    {
      if ( 0 != sb.length() )
      {
        sb.append( separator );
      }
      sb.append( command );
    }
    return sb.toString();
  }

  protected final Properties getDbProperties()
  {
    final Properties properties = new Properties();
    final String username = System.getenv( "TEST_MSSQL_DB_USER" );
    if ( null != username )
    {
      properties.setProperty( "user", username );
    }
    final String password = System.getenv( "TEST_MSSQL_DB_PASSWORD" );
    if ( null != password )
    {
      properties.setProperty( "password", password );
    }
    return properties;
  }

  protected final Driver getDriver()
  {
    return new Driver();
  }

  protected final String getDatabase()
  {
    return getDbURL( DB1_NAME );
  }

  protected final String getControlDatabase()
  {
    return getDbURL( "master" );
  }

  private String getDbURL( final String dbName )
  {
    final String host = System.getProperty( "test.mssql.host", "127.0.0.1" );
    final String port = System.getProperty( "test.mssql.port", null );
    final String instance = System.getProperty( "test.mssql.instance", null );
    return "jdbc:jtds:sqlserver://" +
           host +
           ( port == null ? "" : ":" + port ) +
           "/" + dbName +
           ( instance == null ? "" : ";instance=" + instance );
  }

  @BeforeMethod
  public final void setupDatabases()
    throws Exception
  {
    tearDownDatabases();
    executeSQL( "CREATE DATABASE " + DB1_NAME, getControlDatabase() );
  }

  @AfterMethod
  public final void tearDownDatabases()
    throws Exception
  {
    executeSQL(
      "IF EXISTS ( SELECT * FROM sys.master_files WHERE state = 0 AND db_name(database_id) = '" + DB1_NAME + "') " +
      " DROP DATABASE " + DB1_NAME, getControlDatabase() );
  }

  protected final void executeSQL( final String sql, final String database )
    throws SQLException
  {
    final Connection connection1 = getDriver().connect( database, getDbProperties() );
    connection1.createStatement().execute( sql );
    connection1.close();
  }
}