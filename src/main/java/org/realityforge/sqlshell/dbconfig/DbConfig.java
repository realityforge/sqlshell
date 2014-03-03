package org.realityforge.sqlshell.dbconfig;

import java.util.ArrayList;
import java.util.List;
import org.realityforge.sqlshell.SqlShell;

public final class DbConfig
{
  private final List<Login> _logins;
  private boolean _cleanupLogins;
  private final SqlShell _shell;

  public DbConfig( final SqlShell shell )
  {
    _shell = shell;
    _cleanupLogins = false;
    _logins = new ArrayList<Login>();
  }

  public void setCleanupLogins( final boolean cleanupLogins )
  {
    _cleanupLogins = cleanupLogins;
  }

  public void addLogin( final Login l )
  {
    _logins.add( l );
  }

  public void apply()
    throws Exception
  {
    // Create all required logins
    for ( final Login login : _logins )
    {
      // Check if login exists, if not create
      if ( loginExists( login.getName() ) )
      {
        createLogin( login );
      }
      else
      {
        updateLogin( login );
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
    assert ( 0 == _shell.executeUpdate( join( null, "CREATE LOGIN [", login.getName(),
                                              "]" ) ) );
/*
    from = ""
    options = []
    if new_resource.password
      options << "PASSWORD=N'#{new_resource.password}'"
    end
    options << "DEFAULT_DATABASE=[#{new_resource.default_database}]"
    options << "DEFAULT_LANGUAGE=[#{new_resource.default_language}]"
    if new_resource.password
      options << "CHECK_EXPIRATION=OFF"
      options << "CHECK_POLICY=OFF"
      types = ['SQL_LOGIN']
    else
      types = ['WINDOWS_GROUP','WINDOWS_LOGIN']
      from = 'FROM WINDOWS'
    end

    sqlshell_exec "CREATE LOGIN [#{new_resource.login}]" do
      jdbc_url new_resource.jdbc_url
      jdbc_driver new_resource.jdbc_driver
      extra_classpath new_resource.extra_classpath
      jdbc_properties new_resource.jdbc_properties
      command "CREATE LOGIN [#{new_resource.login}] #{from} WITH #{options.join(', ')}"
*/
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
