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
  extends AbstractMssqlTest
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
    final String database = getDbURL( DB1_NAME );
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

  @BeforeMethod
  public final void setupDatabases()
    throws Exception
  {
    tearDownDatabases();
    executeSQL( "CREATE DATABASE " + DB1_NAME, getControlDbUrl() );
  }

  @AfterMethod
  public final void tearDownDatabases()
    throws Exception
  {
    executeSQL(
      "IF EXISTS ( SELECT * FROM sys.master_files WHERE state = 0 AND db_name(database_id) = '" + DB1_NAME + "') " +
      " DROP DATABASE " + DB1_NAME, getControlDbUrl() );
  }

  protected final void executeSQL( final String sql, final String database )
    throws SQLException
  {
    final Connection connection1 = getDriver().connect( database, getDbProperties() );
    connection1.createStatement().execute( sql );
    connection1.close();
  }
}