package org.realityforge.sqlshell.mssql;

import java.util.ArrayList;
import java.util.List;
import org.realityforge.sqlshell.AbstractMssqlTest;
import org.realityforge.sqlshell.SqlShell;
import org.realityforge.sqlshell.data_type.mssql.Database;
import org.realityforge.sqlshell.data_type.mssql.Login;
import org.realityforge.sqlshell.data_type.mssql.ServerConfig;
import org.realityforge.sqlshell.data_type.mssql.User;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

public class RunnerTest
  extends AbstractMssqlTest
{
  private Runner _runner;
  private SqlShell _shell;

  @BeforeTest
  public void createConfig()
  {
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
    final Login login = new Login( "login1", "pwd", null, null );
    if ( _runner.loginExists( login ) )
    {
      _runner.removeLogin( login );
    }
    final List<Login> logins = new ArrayList<>();
    logins.add( login );
    _runner.apply( new ServerConfig( false, logins, false, new ArrayList<Database>() ) );

    assertTrue( _runner.loginExists( login ) );

    assertPasswordMatch( login.getName(), login.getPassword() );

    _runner.removeLogin( login );
    assertFalse( _runner.loginExists( login ) );
  }

  @Test
  public void loginUpdate()
    throws Exception
  {
    final Login login = new Login( "login1", "pwd", null, null );
    if ( _runner.loginExists( login ) )
    {
      _runner.removeLogin( login );
    }
    _runner.createLogin( login );

    final Login updatedLogin = new Login( login.getName(), "pwd2", null, null );
    final List<Login> logins = new ArrayList<>();
    logins.add( updatedLogin );
    _runner.apply( new ServerConfig( false, logins, false, new ArrayList<Database>() ) );

    assertTrue( _runner.loginExists( login ) );
    assertPasswordMatch( login.getName(), updatedLogin.getPassword() );

    _runner.removeLogin( login );
    assertFalse( _runner.loginExists( login ) );
  }

  @Test
  public void testAddDatabase()
    throws Exception
  {
    final List<Database> dbs = new ArrayList<>();
    final Database db = new Database( "test_db1", new ArrayList<User>() );
    if ( _runner.databaseExists( db) )
    {
      _runner.dropDatabase( db );
    }
    dbs.add( db );
    _runner.apply(new ServerConfig( false, new ArrayList<Login>(  ), false, dbs ));
  }

  @Test
  public void testAddDatabaseWithUser()
    throws Exception
  {
    final List<Login> logins = new ArrayList<>();
    final Login l = new Login("login", "pwd", null, null);
    logins.add(l);
    final ArrayList<User> users = new ArrayList<>(  );
    users.add( new User("user", "login" ) );
    final List<Database> dbs = new ArrayList<>();
    dbs.add( new Database( "test_db2", users ) );
    _runner.apply(new ServerConfig( false, logins, false, dbs ));

    _runner.removeLogin( l );
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
    catch ( Exception e )
    {
      fail("Unable to log in as " + loginName + " with password " + password );
    }
  }
}
