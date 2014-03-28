package org.realityforge.sqlshell.mssql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.realityforge.sqlshell.AbstractMssqlTest;
import org.realityforge.sqlshell.SqlShell;
import org.realityforge.sqlshell.data_type.mssql.Database;
import org.realityforge.sqlshell.data_type.mssql.DatabaseRecoveryModel;
import org.realityforge.sqlshell.data_type.mssql.Login;
import org.realityforge.sqlshell.data_type.mssql.ServerConfig;
import org.realityforge.sqlshell.data_type.mssql.User;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

public class RunnerTest
  extends AbstractMssqlTest
{
  private Runner _runner;
  private SqlShell _shell;
  private ObjectMapper _objectMapper;

  @BeforeTest
  public void createConfig()
  {
    _objectMapper = new ObjectMapper();
    _shell = new SqlShell();
    _shell.setDriver( getDriver() );
    copyDbPropertiesToShell( _shell );
    _shell.setDatabase( getControlDbUrl() );
    _runner = new Runner( _shell );
  }

  @Test
  public void loginCreate()
    throws Exception
  {
    final Login login = login( "login1", "pwd" );
    cleanup( login );

    _runner.apply( sc( jLogins( jLogin( login ) ), jDatabases() ) );

    assertTrue( _runner.loginExists( login ) );

    assertPasswordMatch( login.getName(), login.getPassword() );

    _runner.removeLogin( login );
    assertFalse( _runner.loginExists( login ) );
  }

  @Test
  public void loginUpdate()
    throws Exception
  {
    final Login login = login( "login1", "pwd" );
    cleanup( login );

    _runner.createLogin( login );

    _runner.apply( sc( jLogins( jLogin( login.getName(), "newPwd" ) ), jDatabases() ) );

    assertTrue( _runner.loginExists( login ) );
    assertPasswordMatch( login.getName(), "newPwd" );

    cleanup( login );
  }

  @Test
  public void getLogins()
    throws Exception
  {
    final Login login1 = login( "login1", "pwd" );
    final Login login2 = login( "login2", "pwd" );
    cleanup( login1, login2 );

    _runner.apply( sc( jLogins( jLogin( login1 ), jLogin( login2 ) ), jDatabases() ) );

    assertTrue( _runner.loginExists( login1 ) );
    assertTrue( _runner.loginExists( login2 ) );

    final List<Login> existingLogins = _runner.getLogins();
    boolean found1 = false, found2 = false;
    for ( final Login existingLogin : existingLogins )
    {
      if ( existingLogin.getName().equals( login1.getName() ) )
      {
        found1 = true;
      }
      if ( existingLogin.getName().equals( login2.getName() ) )
      {
        found2 = true;
      }
    }
    assertTrue( found1 );
    assertTrue( found2 );
    cleanup( login1, login2 );
  }

  @Test
  public void cleanupLogins()
    throws Exception
  {
    final Login login1 = login( "login1", "pwd" );
    final Login login2 = login( "login2", "pwd" );
    cleanup( login1, login2 );
    _runner.createLogin( login1 );
    _runner.createLogin( login2 );

    final ArrayList<Login> existingLogins = new ArrayList<Login>();
    existingLogins.add( login1 );
    existingLogins.add( login2 );
    final Runner spyRunner = spy( _runner );
    when( spyRunner.getLogins() ).thenReturn( existingLogins );

    assertTrue( _runner.loginExists( login1 ) );
    assertTrue( _runner.loginExists( login2 ) );

    spyRunner.apply( sc( jLogins( jLogin( login1 ) ), a( "remove_unwanted_logins", "true" ), jDatabases() ) );

    assertTrue( _runner.loginExists( login1 ) );
    assertFalse( _runner.loginExists( login2 ) );

    cleanup( login1 );
  }

  @Test
  public void testAddDatabase()
    throws Exception
  {
    final Database db = database( "test_db1",
                                  a( "collation", "SQL_Latin1_General_CP1_CS_AS" ),
                                  a( "recovery_model", DatabaseRecoveryModel.FULL.name() ) );
    cleanup( db );
    assertTrue( !_runner.databaseExists( db ) );
    _runner.apply( sc( jLogins(), jDatabases( jDatabase( db ) ) ) );
    assertTrue( _runner.databaseExists( db ) );
    assertRecoveryModel( db, DatabaseRecoveryModel.FULL.name() );
    assertCollation( db, "SQL_Latin1_General_CP1_CS_AS" );
    cleanup( db );
  }

  @Test
  public void testUpdateDatabase()
    throws Exception
  {
    final Database db = database( "test_db1",
                                  a( "collation", "SQL_Latin1_General_CP1_CS_AS" ),
                                  a( "recovery_model", DatabaseRecoveryModel.FULL.name() ) );
    cleanup( db );

    _runner.apply( sc( jLogins(), jDatabases( jDatabase( db ) ) ) );
    assertRecoveryModel( db, DatabaseRecoveryModel.FULL.name() );
    assertCollation( db, "SQL_Latin1_General_CP1_CS_AS" );

    _runner.apply( sc( jLogins(), jDatabases( jDatabase( "test_db1",
                                                         a( "collation", "SQL_Latin1_General_CP1_CI_AS" ),
                                                         a( "recovery_model",
                                                            DatabaseRecoveryModel.SIMPLE.name() ) ) ) ) );
    assertRecoveryModel( db, DatabaseRecoveryModel.SIMPLE.name() );
    assertCollation( db, "SQL_Latin1_General_CP1_CI_AS" );
    cleanup( db );
  }

  @Test
  public void testAddDatabaseWithUser()
    throws Exception
  {
    fail( "not implemented" );

    final List<Login> logins = new ArrayList<>();
    final Login l = new Login( "login", "pwd", null, null );
    logins.add( l );
    final ArrayList<User> users = new ArrayList<>();
    users.add( new User( "user", "login" ) );
    final List<Database> dbs = new ArrayList<>();
    //dbs.add( new Database( "test_db2", users ) );
    _runner.apply( new ServerConfig( false, logins, false, dbs ) );

    _runner.removeLogin( l );
  }

  private void cleanup( final Database... dbs )
    throws Exception
  {
    for ( final Database db : dbs )
    {
      if ( _runner.databaseExists( db ) )
      {
        _runner.dropDatabase( db );
      }
    }
  }

  private void cleanup( final Login... logins )
    throws Exception
  {
    for ( final Login login : logins )
    {
      if ( _runner.loginExists( login ) )
      {
        _runner.removeLogin( login );
      }
    }
  }

  private void assertPasswordMatch( final String loginName, final String password )
  {
    final SqlShell shell = new SqlShell();
    shell.setDriver( getDriver() );
    copyDbPropertiesToShell( _shell );
    shell.getDbProperties().setProperty( "user", loginName );
    shell.getDbProperties().setProperty( "password", password );
    shell.setDatabase( getControlDbUrl() );
    try
    {
      assertEquals( 1, shell.query( "select 1" ).size() );
    }
    catch ( final Exception e )
    {
      e.printStackTrace();
      fail( "Unable to log in as " + loginName + " with password " + password );
    }
  }

  private void assertCollation( final Database db, final String collation )
  {
    try
    {
      final
      List<Map<String, Object>>
        results = _shell.query( "SELECT DATABASEPROPERTYEX('" + db.getName() + "', 'Collation') SQLCollation" );
      assertEquals( 1, results.size() );
      assertEquals( collation, results.get( 0 ).get( "SQLCollation" ) );
    }
    catch ( final Exception e )
    {
      e.printStackTrace();
      fail( "Unable to determine collation of database " + db.getName() );
    }
  }

  private void assertRecoveryModel( final Database db, final String model )
  {
    try
    {
      final
      List<Map<String, Object>>
        results = _shell.query( "SELECT name, recovery_model_desc " +
                                "      FROM sys.databases " +
                                "      WHERE name = '" + db.getName() + "'" );
      assertEquals( 1, results.size() );
      assertEquals( model, results.get( 0 ).get( "recovery_model_desc" ) );
    }
    catch ( final Exception e )
    {
      e.printStackTrace();
      fail( "Unable to determine recovery model of database " + db.getName() );
    }
  }

  private ServerConfig sc( final String... attributes )
    throws IOException
  {
    System.out.println(jSC( attributes ));
    return ( new ObjectMapper() ).readValue( jSC( attributes ), ServerConfig.class );
  }

  private Database database( final String name, final String... attributes )
    throws IOException
  {
    return _objectMapper.readValue( jDatabase( name, attributes ), Database.class );
  }

  private Login login( final String name, final String password )
    throws IOException
  {
    return _objectMapper.readValue( jLogin( name, password ), Login.class );
  }

  private String jSC( final String... attributes )
    throws IOException
  {
    return e( attributes );
  }

  private String jLogins( final String... logins )
    throws IOException
  {
    return a( "logins", "[" + join( ", ", logins ) + "]" );
  }

  private String jDatabases( final String... dbs )
    throws IOException
  {
    return a( "databases", "[" + join( ", ", dbs ) + "]" );
  }

  private String jDatabase( final String name, final String... extras )
    throws IOException
  {
    final List<String> attributes = new ArrayList<>( extras.length + 1 );
    attributes.add( a( "name", name ) );
    Collections.addAll( attributes, extras );
    return e( attributes.toArray( new String[ attributes.size() ] ) );
  }

  private String jDatabase( final Database db )
    throws IOException
  {
    return _objectMapper.writeValueAsString( db );
  }

  private String jLogin( final Login login )
    throws IOException
  {
    return _objectMapper.writeValueAsString( login );
  }

  private String jLogin( final String name, final String password )
  {
    return e( a( "name", name ), a( "password", password ) );
  }

  private String a( final String name, final String value )
  {
    if ( '[' == value.charAt( 0 ) )
    {
      return "\"" + name + "\": " + value;
    }
    else
    {
      return "\"" + name + "\": \"" + value + "\"";
    }
  }

  private String e( final String... attributes )
  {
    return "{" + join( ", ", attributes ) + "}";
  }

  private String join( final String separator, final String... commands )
  {
    final StringBuilder sb = new StringBuilder();
    for ( final String command : commands )
    {
      if ( 0 != sb.length() && null != separator )
      {
        sb.append( separator );
      }
      sb.append( command );
    }
    return sb.toString();
  }
}
