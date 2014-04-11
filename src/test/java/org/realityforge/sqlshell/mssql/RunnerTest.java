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
import org.realityforge.sqlshell.data_type.mssql.Permission;
import org.realityforge.sqlshell.data_type.mssql.PermissionAction;
import org.realityforge.sqlshell.data_type.mssql.PermissionPermission;
import org.realityforge.sqlshell.data_type.mssql.PermissionSecurableType;
import org.realityforge.sqlshell.data_type.mssql.ServerConfig;
import org.realityforge.sqlshell.data_type.mssql.User;
import org.testng.Assert;
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
    final Login l3 = login( "login3", "pwd" );
    final Database db = database( "test_db1" );
    cleanup( l, l2, l3 );
    cleanup( db );

    // Basic creation
    final User user2 = user( "user2", "login2" );
    _runner.apply( sc( jLogins( jLogin( l ), jLogin( l2 ), jLogin( l3 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login1" ), jUser( user2 ) ) ) ) ) );

    assertUserMatch( db.getName(), "login1", "user1" );
    assertUserMatch( db.getName(), "login2", "user2" );

    _runner.apply( sc( jLogins( jLogin( l ), jLogin( l2 ), jLogin( l3 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login3" ) ) ) ) ) );

    assertUserMatch( db.getName(), "login3", "user1" );
    assertUserMatch( db.getName(), "login2", "user2" ); // Should not autodelete

    // Should delete if 'delete unmanaged users' is true
    _runner.apply( sc( a( "delete_unmanaged_users", "true" ),
                       jLogins( jLogin( l ), jLogin( l2 ), jLogin( l3 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login3" ) ) ) ) ) );
    assertUserMatch( db.getName(), "login3", "user1" );
    assertFalse( _runner.userExists( db, user2 ) );

    // should delete if database is managed
    _runner.apply( sc( jLogins( jLogin( l ), jLogin( l2 ), jLogin( l3 ) ),
                       jDatabases( jDatabase( db.getName(), a( "managed", "true" ) ) ) ) );
    assertFalse( _runner.userExists( db, user( "user1", "login3" ) ) );

    cleanup( db );
    cleanup( l, l2, l3 );
  }

  @Test
  public void testDatabaseRoles()
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

  @Test
  public void testPermissions()
    throws Exception
  {
    final Login l1 = login( "login1", "pwd" );
    final Login l2 = login( "login2", "pwd" );
    final Database db = database( "test_db1" );
    cleanup( l1, l2 );
    cleanup( db );

    _runner.apply( sc( jLogins( jLogin( l1 ), jLogin( l2 ) ), jDatabases( jDatabase( db ) ) ) );

    _shell.execute( "USE test_db1; execute('create PROCEDURE aaa AS SELECT 1')" );

    final Permission grantConnect = permission( a( "permission", PermissionPermission.CONNECT.toString() ),
                                                a( "securable_type", PermissionSecurableType.DATABASE.toString() ) );
    final Permission grantBackupDB = permission( a( "permission", PermissionPermission.BACKUP_DATABASE.toString() ),
                                                 a( "securable_type", PermissionSecurableType.DATABASE.toString() ) );
    final Permission grantExecuteAAA = permission( a( "permission", PermissionPermission.EXECUTE.toString() ),
                                                   a( "securable_type", PermissionSecurableType.OBJECT.toString() ),
                                                   a( "securable", "aaa" ) );
    final Permission grantBackupLog = permission( a( "permission", PermissionPermission.BACKUP_LOG.toString() ),
                                                  a( "securable_type", PermissionSecurableType.DATABASE.toString() ) );
    final Permission denyExecuteAAA = permission( a( "action", PermissionAction.DENY.toString() ),
                                                  a( "permission", PermissionPermission.EXECUTE.toString() ),
                                                  a( "securable_type", PermissionSecurableType.OBJECT.toString() ),
                                                  a( "securable", "aaa" ) );

    _runner.apply( sc( jLogins( jLogin( l1 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login1",
                                jPermissions( jPermission( grantBackupDB ),
                                              jPermission( grantExecuteAAA ),
                                              jPermission( grantConnect ) ) ),
                         jUser( "user2", "login2",
                                jPermissions( jPermission( grantExecuteAAA )
                                ) ) ) ) ) ) );

    assertTrue( hasGrantedPermission( db.getName(), "user1", grantConnect ) );
    assertTrue( hasGrantedPermission( db.getName(), "user1", grantBackupDB ) );
    assertTrue( hasGrantedPermission( db.getName(), "user1", grantExecuteAAA ) );
    assertTrue( hasGrantedPermission( db.getName(), "user2", grantExecuteAAA ) );
    assertFalse( hasPermission( db.getName(), "user1", grantBackupLog ) );

    _runner.apply( sc( jLogins( jLogin( l1 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login1",
                                jPermissions( jPermission( grantConnect ),
                                              jPermission( grantBackupLog ),
                                              jPermission( denyExecuteAAA ) ) ) ) ) ) ) );

    assertTrue( hasGrantedPermission( db.getName(), "user1", grantConnect ) );
    // Check permissions are not deleted in default configuration
    assertTrue( hasGrantedPermission( db.getName(), "user1", grantBackupDB ) );
    assertTrue( hasGrantedPermission( db.getName(), "user1", grantBackupLog ) );
    assertTrue( hasDeniedPermission( db.getName(), "user1", denyExecuteAAA ) );
    assertFalse( hasGrantedPermission( db.getName(), "user1", grantExecuteAAA ) );
    assertTrue( hasGrantedPermission( db.getName(), "user2", grantExecuteAAA ) );

    // Test revoke
    _runner.apply( sc( jLogins( jLogin( l1 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login1",
                                jPermissions(
                                  jPermission( a( "action", PermissionAction.REVOKE.toString() ),
                                               a( "permission", PermissionPermission.EXECUTE.toString() ),
                                               a( "securable_type", PermissionSecurableType.OBJECT.toString() ),
                                               a( "securable", "aaa" ) )
                                ) ) ) ) ) ) );

    assertTrue( hasGrantedPermission( db.getName(), "user1", grantConnect ) );
    assertTrue( hasGrantedPermission( db.getName(), "user1", grantBackupDB ) );
    assertTrue( hasGrantedPermission( db.getName(), "user1", grantBackupLog ) );
    assertFalse( hasPermission( db.getName(), "user1", grantExecuteAAA ) );
    assertFalse( hasPermission( db.getName(), "user1", denyExecuteAAA ) );
    assertTrue( hasGrantedPermission( db.getName(), "user2", grantExecuteAAA ) );

    // Test that deletion of permissions works if database is 'managed'
    _runner.apply( sc( jLogins( jLogin( l1 ) ),
                       jDatabases( jDatabase( db.getName(), a( "managed", "true" ), jUsers(
                         jUser( "user1", "login1",
                                jPermissions( jPermission( grantConnect ),
                                              jPermission( grantBackupLog ),
                                              jPermission( denyExecuteAAA ) ) ) ) ) ) ) );

    assertTrue( hasGrantedPermission( db.getName(), "user1", grantConnect ) );
    assertFalse( hasPermission( db.getName(), "user1", grantBackupDB ) );
    assertTrue( hasGrantedPermission( db.getName(), "user1", grantBackupLog ) );
    assertTrue( hasDeniedPermission( db.getName(), "user1", denyExecuteAAA ) );
    assertFalse( hasPermission( db.getName(), "user2", grantExecuteAAA ) );

    // Test that deletion of permissions works if 'delete unmanaged permissions' is true and db is unmanaged
    _runner.apply( sc( a( "delete_unmanaged_permissions", "true" ),
                       jLogins( jLogin( l1 ) ),
                       jDatabases( jDatabase( db.getName(), jUsers(
                         jUser( "user1", "login1" ) ) ) ) ) );

    assertFalse( hasPermission( db.getName(), "user1", grantConnect ) );
    assertFalse( hasPermission( db.getName(), "user1", grantBackupLog ) );
    assertFalse( hasPermission( db.getName(), "user1", denyExecuteAAA ) );

    Assert.assertEquals(
      _shell.query( "USE [" + db.getName() + "];" + _runner.userPermissionsSQL( db.getName(), "user1", null ) ).size(),
      0 );

    Assert.assertEquals(
      _shell.query( "USE [" + db.getName() + "];" + _runner.userPermissionsSQL( db.getName(), "user2", null ) ).size(),
      0 );

    cleanup( db );
    cleanup( l1, l2 );
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

  private boolean hasDeniedPermission( final String database,
                                       final String user,
                                       final Permission permission )
    throws Exception
  {
    return _shell.query( "USE [" + database + "];" +
                         _runner.userHasPermissionSQL( database, user, permission, PermissionAction.DENY ) ).size() ==
           1;
  }

  private boolean hasGrantedPermission( final String database,
                                        final String user,
                                        final Permission permission )
    throws Exception
  {
    return _shell.query( "USE [" + database + "];" +
                         _runner.userHasPermissionSQL( database, user, permission, PermissionAction.GRANT ) ).size() ==
           1;
  }

  private boolean hasPermission( final String database,
                                 final String user,
                                 final Permission permission )
    throws Exception
  {
    return hasGrantedPermission( database, user, permission ) || hasDeniedPermission( database, user, permission );
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
    return _objectMapper.readValue( jLogin( name, password, extras ), Login.class );
  }

  private User user( final String name, final String login, final String... extras )
    throws IOException
  {
    return _objectMapper.readValue( jUser( name, login, extras ), User.class );
  }

  private Permission permission( final String... attributes )
    throws IOException
  {
    return _objectMapper.readValue( jPermission( attributes ), Permission.class );
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

  private String jUser( final User user )
    throws IOException
  {
    return _objectMapper.writeValueAsString( user );
  }

  private String jLogin( final Login login )
    throws IOException
  {
    return _objectMapper.writeValueAsString( login );
  }

  private String jPermission( final Permission permission )
    throws IOException
  {
    return _objectMapper.writeValueAsString( permission );
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

  private String jPermissions( final String... permissions )
  {
    return a( "permissions", "[" + join( ", ", permissions ) + "]" );
  }

  private String jPermission( final String... attributes )
  {
    return e( attributes );
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
