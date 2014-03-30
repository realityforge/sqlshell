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
import org.realityforge.sqlshell.data_type.mssql.LoginServerRole;
import org.realityforge.sqlshell.data_type.mssql.ServerConfig;
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
    final Login login = login( "login1", "pwd", a( "server_roles", "[\"" + LoginServerRole.SYSADMIN + "\",\"" +
                                                                   LoginServerRole.DBCREATOR + "\"]" ) );
    cleanup( login );

    _runner.apply( sc( jLogins( jLogin( login ) ), jDatabases() ) );

    assertTrue( _runner.loginExists( login ) );

    assertPasswordMatch( login.getName(), login.getPassword() );
    assertTrue( hasServerRole( login.getName(), LoginServerRole.SYSADMIN ) );
    assertTrue( hasServerRole( login.getName(), LoginServerRole.DBCREATOR ) );

    _runner.removeLogin( login );
    assertFalse( _runner.loginExists( login ) );
  }

  @Test
  public void loginUpdate()
    throws Exception
  {
    final Login login = login( "login1", "pwd", a( "server_roles", "[\"" + LoginServerRole.BULKADMIN + "\",\"" +
                                                                   LoginServerRole.DBCREATOR + "\"]" ) );
    cleanup( login );

    _runner.apply( sc( jLogins( jLogin( login ) ), jDatabases() ) );

    assertTrue( hasServerRole( login.getName(), LoginServerRole.BULKADMIN ) );
    assertTrue( hasServerRole( login.getName(), LoginServerRole.DBCREATOR ) );

    _runner.apply( sc( jLogins( jLogin( login.getName(), "newPwd",
                                        a( "server_roles", "[\"" + LoginServerRole.SYSADMIN + "\",\"" +
                                                           LoginServerRole.DBCREATOR + "\"]" ) ) ), jDatabases() ) );

    assertTrue( hasServerRole( login.getName(), LoginServerRole.SYSADMIN ) );
    assertTrue( hasServerRole( login.getName(), LoginServerRole.DBCREATOR ) );
    assertFalse( hasServerRole( login.getName(), LoginServerRole.BULKADMIN ) );

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

    final List<String> existingLogins = _runner.getLogins();
    boolean found1 = false, found2 = false;
    for ( final String existingLogin : existingLogins )
    {
      if ( existingLogin.equals( login1.getName() ) )
      {
        found1 = true;
      }
      if ( existingLogin.equals( login2.getName() ) )
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

    final ArrayList<String> existingLogins = new ArrayList<>();
    existingLogins.add( login1.getName() );
    existingLogins.add( login2.getName() );
    final Runner spyRunner = spy( _runner );
    when( spyRunner.getLogins() ).thenReturn( existingLogins );

    assertTrue( _runner.loginExists( login1 ) );
    assertTrue( _runner.loginExists( login2 ) );

    spyRunner.apply( sc( jLogins( jLogin( login1 ) ), a( "delete_unmanaged_logins", "false" ), jDatabases() ) );

    assertTrue( _runner.loginExists( login1 ) );
    assertTrue( _runner.loginExists( login2 ) );

    spyRunner.apply( sc( jLogins( jLogin( login1 ) ), a( "delete_unmanaged_logins", "true" ), jDatabases() ) );

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
  public void testCleanupDatabases()
    throws Exception
  {
    final Database db1 = database( "test_db1" );
    final Database db2 = database( "test_db2" );
    cleanup( db1, db2 );

    _runner.apply( sc( jLogins(), jDatabases( jDatabase( db1 ), jDatabase( db2 ) ) ) );

    assertTrue( _runner.databaseExists( db1 ) );
    assertTrue( _runner.databaseExists( db2 ) );

    final ArrayList<Database> existingDbs = new ArrayList<>();
    existingDbs.add( db1 );
    existingDbs.add( db2 );
    final Runner spyRunner = spy( _runner );
    when( spyRunner.getDatabases() ).thenReturn( existingDbs );

    spyRunner.apply( sc( jLogins(), jDatabases( jDatabase( db1 ) ), a( "delete_unmanaged_databases", "false" ) ) );

    assertTrue( _runner.databaseExists( db1 ) );
    assertTrue( _runner.databaseExists( db2 ) );

    spyRunner.apply( sc( jLogins(), jDatabases( jDatabase( db1 ) ), a( "delete_unmanaged_databases", "true" ) ) );

    assertTrue( _runner.databaseExists( db1 ) );
    assertFalse( _runner.databaseExists( db2 ) );

    cleanup( db1 );
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
  public void testUsers()
    throws Exception
  {
    final Login l = login( "login1", "pwd" );
    final Login l2 = login( "login2", "pwd" );
    final Database db = database( "test_db1" );
    cleanup( l, l2 );
    cleanup( db );

    _runner.apply( sc( jLogins( jLogin( l ), jLogin( l2 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login1", a( "roles", "[\"db_datareader\",\"db_datawriter\"]" ) ) ) ) ) ) );

    assertUserMatch( db.getName(), "login1", "user1" );
    assertTrue( hasDatabaseRole( db.getName(), "user1", "db_datareader" ) );
    assertTrue( hasDatabaseRole( db.getName(), "user1", "db_datawriter" ) );
    assertFalse( hasDatabaseRole( db.getName(), "user1", "db_ddladmin" ) );

    _runner.apply( sc( jLogins( jLogin( l ), jLogin( l2 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login2", a( "roles", "[\"db_datareader\",\"db_ddladmin\"]" ) ) ) ) ) ) );

    assertUserMatch( db.getName(), "login2", "user1" );
    assertTrue( hasDatabaseRole( db.getName(), "user1", "db_datareader" ) );
    assertFalse( hasDatabaseRole( db.getName(), "user1", "db_datawriter" ) );
    assertTrue( hasDatabaseRole( db.getName(), "user1", "db_ddladmin" ) );

    cleanup( db );
    cleanup( l, l2 );
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

  private void assertUserMatch( final String dbName, String loginName, final String userName )
  {
    try
    {
      assertEquals( 1, _shell.query( "SELECT U.name AS [user], SP.name AS [login] " +
                                     "FROM [" + dbName + "].sys.database_principals U" +
                                     "  JOIN sys.server_principals SP ON SP.sid = U.sid AND SP.is_disabled = 0 " +
                                     "WHERE" +
                                     "  U.type_desc IN ('SQL_USER','WINDOWS_USER','WINDOWS_GROUP') AND" +
                                     "  U.name = '" + userName + "' AND" +
                                     "  SP.name = '" + loginName + "'" ).size() );
    }
    catch ( final Exception e )
    {
      e.printStackTrace();
      fail( "Unable to find user " + userName + " for login " + loginName + " in database " + dbName );
    }
  }

  private void assertCollation( final Database db, final String collation )
  {
    try
    {
      final List<Map<String, Object>> results =
        _shell.query( "SELECT DATABASEPROPERTYEX('" + db.getName() + "', 'Collation') SQLCollation" );
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
      final List<Map<String, Object>> results =
        _shell.query( "SELECT name, recovery_model_desc FROM sys.databases WHERE name = '" + db.getName() + "'" );
      assertEquals( 1, results.size() );
      assertEquals( model, results.get( 0 ).get( "recovery_model_desc" ) );
    }
    catch ( final Exception e )
    {
      e.printStackTrace();
      fail( "Unable to determine recovery model of database " + db.getName() );
    }
  }

  private boolean hasServerRole( final String name, final LoginServerRole role )
    throws Exception
  {
    return _shell.query( _runner.loginHasServerRoleSQL( name, role ) ).size() == 1;
  }

  private boolean hasDatabaseRole( final String database, final String user, final String role )
    throws Exception
  {
    return _shell.query( _runner.userHasRoleSQL( database, user, role ) ).size() == 1;
  }

  private ServerConfig sc( final String... attributes )
    throws IOException
  {
    return ( new ObjectMapper() ).readValue( jSC( attributes ), ServerConfig.class );
  }

  private Database database( final String name, final String... attributes )
    throws IOException
  {
    return _objectMapper.readValue( jDatabase( name, attributes ), Database.class );
  }

  private Login login( final String name, final String password, final String... extras )
    throws IOException
  {
    System.out.println( jLogin( name, password, extras ) );
    return _objectMapper.readValue( jLogin( name, password, extras ), Login.class );
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

  private String jUsers( final String... users )
  {
    return a( "users", "[" + join( ", ", users ) + "]" );
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

  private String jLogin( final String name, final String password, final String... extras )
  {
    final List<String> attributes = new ArrayList<>( extras.length + 2 );
    attributes.add( a( "name", name ) );
    attributes.add( a( "password", password ) );
    Collections.addAll( attributes, extras );
    return e( attributes.toArray( new String[ attributes.size() ] ) );
  }

  private String jUser( final String name, final String login, final String... extras )
  {
    final List<String> attributes = new ArrayList<>( extras.length + 2 );
    attributes.add( a( "name", name ) );
    attributes.add( a( "login", login ) );
    Collections.addAll( attributes, extras );
    return e( attributes.toArray( new String[ attributes.size() ] ) );
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
