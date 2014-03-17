package org.realityforge.sqlshell;

import java.util.Enumeration;
import java.util.Properties;
import net.sourceforge.jtds.jdbc.Driver;

public abstract class AbstractMssqlTest
{
  protected final void copyDbPropertiesToShell( final SqlShell shell )
  {
    Properties props = getDbProperties();
    Enumeration e = props.propertyNames();
    while ( e.hasMoreElements() )
    {
      String key = (String) e.nextElement();
      shell.getDbProperties().setProperty( key, props.getProperty( key ) );
    }
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

  protected final String getControlDbUrl()
  {
    return getDbURL( "master" );
  }

  protected final String getDbURL( final String dbName )
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
}
