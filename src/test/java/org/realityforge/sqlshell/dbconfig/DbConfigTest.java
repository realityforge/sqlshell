package org.realityforge.sqlshell.dbconfig;

import java.util.ArrayList;
import java.util.Properties;
import net.sourceforge.jtds.jdbc.Driver;
import org.realityforge.sqlshell.AbstractMssqlTest;
import org.realityforge.sqlshell.SqlShell;
import org.realityforge.sqlshell.data_type.mssql.Database;
import org.realityforge.sqlshell.data_type.mssql.Login;
import org.realityforge.sqlshell.data_type.mssql.User;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DbConfigTest
  extends AbstractMssqlTest
{
  private SqlShell _shell;
  private DbConfig _config;

  @BeforeTest
  public void createConfig()
  {
    _shell = new SqlShell();
    _shell.setDriver( getDriver() );
    copyDbPropertiesToShell( _shell );
    _shell.setDatabase( getControlDbUrl() );
    _config = new DbConfig( _shell );
  }

  @Test
  public void testAddLogin()
    throws Exception
  {
    DbConfig config = new DbConfig(_shell);
    config.addLogin( new Login( "login1", "pwd", null, null ) );
    config.apply();
  }

  @Test
  public void testAddDatabase()
    throws Exception
  {
    DbConfig config = new DbConfig(_shell);
    config.addDatabase( new Database( "test_db1", new ArrayList<User>() ) );
    config.apply();
  }

  @Test
  public void testAddDatabaseWithUser()
    throws Exception
  {
    DbConfig config = new DbConfig(_shell);
    final Login l = new Login("login", "pwd", null, null);
    final ArrayList<User> users = new ArrayList<>(  );
    users.add( new User("user", l ) );
    config.addDatabase( new Database( "test_db2", users ) );
    config.apply();
  }
}
