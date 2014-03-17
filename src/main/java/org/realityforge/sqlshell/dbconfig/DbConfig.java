package org.realityforge.sqlshell.dbconfig;

import java.util.ArrayList;
import java.util.List;
import javax.xml.crypto.Data;
import org.realityforge.sqlshell.SqlShell;
import org.realityforge.sqlshell.data_type.mssql.Database;
import org.realityforge.sqlshell.data_type.mssql.Login;

public final class DbConfig
{
  private final List<Database> _databases;
  private boolean _cleanupDatabases;
  private final List<Login> _logins;
  private boolean _cleanupLogins;
  private final SqlShell _shell;

  public DbConfig( final SqlShell shell )
  {
    _shell = shell;
    _cleanupLogins = false;
    _logins = new ArrayList<>();
    _databases = new ArrayList<>();
  }

  public void setCleanupLogins( final boolean cleanupLogins )
  {
    _cleanupLogins = cleanupLogins;
  }

  public void setCleanupDatabases( final boolean cleanupDatabases )
  {
    _cleanupDatabases = cleanupDatabases;
  }

  public void addDatabase( final Database d )
  {
    _databases.add( d );
  }

  public void addLogin( final Login l )
  {
    _logins.add( l );
  }

  public void apply()
    throws Exception
  {
    // Create all required databases

    // Create all required logins
    for ( final Login login : _logins )
    {
      // Check if login exists, if not create
      if ( loginExists( login.getName() ) )
      {
        updateLogin( login );
      }
      else
      {
        createLogin( login );
      }
    }

    // Remove any unwanted ones
    final List<Login> existingLogins = getLogins();

    for ( final Login existingLogin : existingLogins )
    {
      if ( !_logins.contains( existingLogin ) )
      {
        if ( _cleanupLogins )
        {
          removeLogin( existingLogin );
        }
        else
        {
          log( "Existing login ", existingLogin.getName(), " is not in configuration" );
        }
      }
    }
  }

  private void removeLogin( final Login existingLogin )
  {
    throw new UnsupportedOperationException( "existingLogin" );
  }

  private void updateLogin( final Login login )
  {
    throw new UnsupportedOperationException( "updateLogin" );
  }

  private void createLogin( final Login login )
    throws Exception
  {
    String options = join(", ",
                          "DEFAULT_DATABASE=[" + (login.getDefaultDatabase() == null ? "master" : login.getDefaultDatabase()) + "]",
                          "DEFAULT_LANGUAGE=[" + (login.getDefaultLanguage() == null ? "us_english" : login.getDefaultLanguage()) + "]");
    String from = "";
    if ( null != login.getPassword() )
    {
      options = join(", ",
                     options,
                     "PASSWORD = N'" + login.getPassword() + "'",
                     "CHECK_EXPIRATION=OFF",
                     "CHECK_POLICY=OFF" );
    }
    else
    {
      from = "FROM WINDOWS";
    }
    assert ( 0 == _shell.executeUpdate( "CREATE LOGIN [" + login.getName() + "] " + from + " WITH " + options ) );
  }

  private boolean loginExists( final String name )
  {
    return false;
  }

  private void log( final String... s )
  {
    System.out.println( join( null, s ) );
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

  public List<Login> getLogins()
  {
    return new ArrayList<Login>();
  }
}
