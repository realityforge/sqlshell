package org.realityforge.sqlshell.dbconfig;

import net.sourceforge.jtds.jdbc.Driver;
import org.realityforge.sqlshell.SqlShell;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DbConfigTest
{
  private SqlShell _shell;
  private DbConfig _config;

  @BeforeTest
  public void createConfig()
  {
    _shell = new SqlShell();
    final Driver driver = new Driver();
    _shell.setDriver( driver );
    _config = new DbConfig( _shell );
  }

  @Test
  public void testAddLogin()
  {
    DbConfig config = new DbConfig();
  }

}
