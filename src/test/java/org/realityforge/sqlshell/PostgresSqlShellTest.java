package org.realityforge.sqlshell;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.postgresql.Driver;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

@SuppressWarnings( "UnnecessaryLocalVariable" )
public class PostgresSqlShellTest
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

    final String schema = "x";
    final String table = "myTable";
    final String ddl =
      s( schema( schema ),
         table( schema, table, column( "ID", "integer" ), column( "TS", "timestamp NOT NULL" ) ) );

    final int rowCount = shell.execute( ddl );
    assertEquals( rowCount, 0 );

    final List<Map<String, Object>> results =
      shell.query( "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'x' ORDER BY table_schema,table_name" );

    assertEquals( results.size(), 1 );
    assertEquals( results.get( 0 ).get( "table_schema" ), schema );
    assertEquals( results.get( 0 ).get( "table_name" ), table );
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

  protected final String s( final String... commands )
  {
    return join( ';', commands );
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
    final String username = System.getenv( "TEST_PG_DB_USER" );
    if ( null != username )
    {
      properties.setProperty( "user", username );
    }
    final String password = System.getenv( "TEST_PG_DB_PASSWORD" );
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
    return getBaseDbURL() + DB1_NAME;
  }

  protected final String getControlDatabase()
  {
    return getBaseDbURL() + "postgres";
  }

  private String getBaseDbURL()
  {
    final String host = System.getProperty( "test.psql.host", "127.0.0.1" );
    final String port = System.getProperty( "test.psql.port", "5432" );
    return "jdbc:postgresql://" + host + ":" + port + "/";
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
    executeSQL( "DROP DATABASE IF EXISTS " + DB1_NAME, getControlDatabase() );
  }

  protected final void executeSQL( final String sql, final String database )
    throws SQLException
  {
    final Connection connection1 = getDriver().connect( database, getDbProperties() );
    connection1.createStatement().execute( sql );
    connection1.close();
  }
}