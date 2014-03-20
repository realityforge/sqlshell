package org.realityforge.sqlshell.mssql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.realityforge.sqlshell.SqlShell;
import org.realityforge.sqlshell.data_type.mssql.Database;
import org.realityforge.sqlshell.data_type.mssql.Login;
import org.realityforge.sqlshell.data_type.mssql.ServerConfig;

public class Runner
{
  private final SqlShell _shell;

  public Runner( final SqlShell shell )
  {
    _shell = shell;
  }

  public void apply( final ServerConfig config )
    throws Exception
  {
    // Create all required databases

    // Create all required logins
    for ( final Login login : config.getLogins() )
    {
      // Check if login exists, if not create
      if ( loginExists( login ) )
      {
        updateLogin( login );
      }
      else
      {
        createLogin( login );
      }
    }

    // Remove any unwanted ones
    if ( config.isRemoveUnwantedLogins() )
    {
      for ( final Login existingLogin : getLogins() )
      {
        if ( !config.getLogins().contains( existingLogin ) )
        {
          removeLogin( existingLogin );
        }
      }
    }
  }

  protected List<Login> getLogins()
    throws Exception
  {
    // TODO: Implement to obtain logins from server
    final List<Map<String,Object>> loginRows = _shell.query(
      "SELECT SP.name as name " +
      "FROM " +
      "  sys.syslogins L " +
      "JOIN sys.server_principals SP ON SP.sid = L.sid " +
      "WHERE " +
      "  SP.type_desc IN ('SQL_LOGIN', 'WINDOWS_GROUP', 'WINDOWS_LOGIN') AND " +
      "  SP.is_disabled = 0 AND " +
      "  SP.name NOT LIKE 'NT AUTHORITY\\%' AND " +
      "  SP.name NOT LIKE 'NT SERVICE\\%'"
    );

    final ArrayList<Login> logins = new ArrayList<>();

    for ( final Map<String, Object> loginRow : loginRows )
    {
      logins.add( new Login( (String) loginRow.get( "name" ), null, null, null));
    }
    return logins;
  }

  public boolean loginExists( final Login login )
    throws Exception
  {
    final String types;
    if ( null != login.getPassword() )
    {
      types = "'SQL_LOGIN'";
    }
    else
    {
      types = "'WINDOWS_GROUP','WINDOWS_LOGIN'";
    }

    return !_shell.query( "SELECT * FROM sys.server_principals WHERE name = '" + login.getName() +
                          "' AND type_desc IN (" + types + ")" ).isEmpty();
  }

  public void removeLogin( final Login login )
    throws Exception
  {
    log("Removing login ", login.getName() );

    _shell.executeUpdate( "DROP LOGIN [" + login.getName() + "]" );
  }

  public void updateLogin( final Login login )
    throws Exception
  {
    log("Updating login ", login.getName() );
    _shell.executeUpdate( "ALTER LOGIN [" + login.getName() + "] WITH " + loginOptions( login ) );
  }

  public void createLogin( final Login login )
    throws Exception
  {
    final String from;
    if ( null == login.getPassword() )
    {
      from = "FROM WINDOWS";
    }
    else
    {
      from = "";
    }

    log("Creating login ", login.getName() );

    _shell.executeUpdate( "CREATE LOGIN [" + login.getName() + "] " + from + " WITH " + loginOptions( login ) );
  }

  private String loginOptions( final Login login )
  {
    String options = "";

    if ( null != login.getPassword() )
    {
      options = join( ", ",
                      "PASSWORD = N'" + login.getPassword() + "'",
                      "CHECK_EXPIRATION=OFF",
                      "CHECK_POLICY=OFF" );
    }

    options = join( ", ",
                    options,
                    "DEFAULT_DATABASE=[" +
                    ( login.getDefaultDatabase() == null ? "master" : login.getDefaultDatabase() ) +
                    "]",
                    "DEFAULT_LANGUAGE=[" +
                    ( login.getDefaultLanguage() == null ? "us_english" : login.getDefaultLanguage() ) +
                    "]" );

    return options;
  }

  public boolean databaseExists( final Database db )
  {
    return false;
  }

  public void dropDatabase( final Database db )
  {
    throw new UnsupportedOperationException( "dropDatabase" );
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
}
