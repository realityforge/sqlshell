package org.realityforge.sqlshell.mssql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.realityforge.sqlshell.SqlShell;
import org.realityforge.sqlshell.data_type.mssql.Database;
import org.realityforge.sqlshell.data_type.mssql.Login;
import org.realityforge.sqlshell.data_type.mssql.LoginServerRole;
import org.realityforge.sqlshell.data_type.mssql.Permission;
import org.realityforge.sqlshell.data_type.mssql.PermissionAction;
import org.realityforge.sqlshell.data_type.mssql.PermissionPermission;
import org.realityforge.sqlshell.data_type.mssql.PermissionSecurableType;
import org.realityforge.sqlshell.data_type.mssql.ServerConfig;
import org.realityforge.sqlshell.data_type.mssql.User;

public class Runner
{
  private final SqlShell _shell;

  private List<String> SYS_DATABASES = Arrays.asList( "master", "msdb", "model", "tempdb" );

  public Runner( final SqlShell shell )
  {
    _shell = shell;
  }

  public void apply( final ServerConfig config )
    throws Exception
  {
    // Create all required logins
    for ( final Login login : config.getLogins() )
    {
      // Check if login exists, if not create
      if ( !loginExists( login ) )
      {
        createLogin( login );
      }
      updateLogin( login );
    }

    // Create all required databases
    for ( final Database db : config.getDatabases() )
    {
      if ( !SYS_DATABASES.contains( db.getName() ) )
      {
        if ( !databaseExists( db ) )
        {
          createDatabase( db );
        }
        alterDatabase( db, config );
      }
    }

    removeUnwantedDatabases( config );
    removeUnwantedServerRoles( config );
    removeUnwantedLogins( config );
  }

  private void removeUnwantedDatabases( final ServerConfig config )
    throws Exception
  {
    if ( Boolean.TRUE.equals( config.isDeleteUnmanagedDatabases() ) )
    {
      for ( final Database existingDb : getDatabases() )
      {
        boolean keep = false;
        for ( final Database db : config.getDatabases() )
        {
          if ( db.getName().equals( existingDb.getName() ) )
          {
            keep = true;
            break;
          }
        }
        if ( !keep )
        {
          dropDatabase( existingDb );
        }
      }
    }
  }

  private void removeUnwantedLogins( final ServerConfig config )
    throws Exception
  {
    if ( Boolean.TRUE.equals( config.isDeleteUnmanagedLogins() ) )
    {
      for ( final String existingLogin : getLogins() )
      {
        boolean keep = false;
        for ( final Login login : config.getLogins() )
        {
          if ( login.getName().equals( existingLogin ) )
          {
            keep = true;
            break;
          }
        }
        if ( !keep )
        {
          removeLogin( new Login( existingLogin, null, null, null, null ) );
        }
      }
    }
  }

  private void removeUnwantedServerRoles( final ServerConfig config )
    throws Exception
  {
    if ( !Boolean.TRUE.equals( config.isDeleteUnmanagedServerRoles() ) )
    {
      return;
    }

    // Collect all roles that should exist and store against login name
    final Map<String, List<LoginServerRole>> wantedRoles = new HashMap<>();
    for ( final Login login : config.getLogins() )
    {
      final List<LoginServerRole> loginRoles = login.getServerRoles();
      if ( null == loginRoles || loginRoles.size() == 0 )
      {
        continue;
      }
      wantedRoles.put( login.getName().toLowerCase(), loginRoles );
    }

    final List<Map<String, Object>> existingRoles = _shell.query( allServerRolesSQL() );
    for ( final Map<String, Object> existingRoleRow : existingRoles )
    {
      final String login = (String) existingRoleRow.get( "login" );
      final String roleName = (String) existingRoleRow.get( "role" );

      if ( "sa".equalsIgnoreCase( login ) || login.equalsIgnoreCase( (String)_shell.getDbProperties().get("user") ) )
      {
        continue;
      }

      if ( !wantedRoles.containsKey( login ) ||
           !wantedRoles.get( login ).contains( LoginServerRole.valueOf( roleName.toUpperCase() ) ) )
      {
        removeLoginRole( login, roleName );
      }
    }
  }

  protected List<String> getLogins()
    throws Exception
  {
    final List<Map<String, Object>> loginRows = _shell.query(
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

    final ArrayList<String> logins = new ArrayList<>();

    for ( final Map<String, Object> loginRow : loginRows )
    {
      logins.add( (String) loginRow.get( "name" ) );
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
    log( "Removing login ", login.getName() );

    _shell.executeUpdate( "DROP LOGIN [" + login.getName() + "]" );
  }

  public void updateLogin( final Login login )
    throws Exception
  {
    log( "Updating login ", login.getName() );
    _shell.executeUpdate( "ALTER LOGIN [" + login.getName() + "] WITH " + loginOptions( login ) );

    // Set up database level permissions for login
    if ( null != login.getServerRoles() )
    {
      for ( final LoginServerRole loginServerRole : login.getServerRoles() )
      {
        ensureLoginRole( login.getName(), loginServerRole );
      }
    }
  }

  private void removeLoginRole( final String login, final String role )
    throws Exception
  {
    log( "Removing role " + role + " from " + login );

    _shell.execute(
      "EXEC sys.sp_dropsrvrolemember @loginame = N'" + login + "', @rolename = N'" + role + "'" );
  }

  private void ensureLoginRole( final String loginName, final LoginServerRole role )
    throws Exception
  {
    final String loginRoleExistsSql = loginHasServerRoleSQL( loginName, role );

    if ( 0 == _shell.query( loginRoleExistsSql ).size() )
    {
      log( "Granting role " + role + " for login " + loginName );
      _shell.executeUpdate(
        "EXEC sys.sp_addsrvrolemember @loginame = N'" + loginName + "', @rolename = N'" + role + "'" );
    }
  }

  protected String loginHasServerRoleSQL( final String loginName, final LoginServerRole role )
  {
    return loginServerRolesSQL( loginName ) + " AND RP.name = N'" + role + "'";
  }

  private String loginServerRolesSQL( final String loginName )
  {
    return "SELECT RP.name FROM sys.server_principals P " +
           "JOIN sys.server_role_members SRM ON SRM.member_principal_id = P.principal_id " +
           "JOIN sys.server_principals RP ON RP.principal_id = SRM.role_principal_id AND RP.type_desc = 'SERVER_ROLE' " +
           "WHERE P.name = N'" + loginName + "'";
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

    log( "Creating login ", login.getName() );

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

  protected List<Database> getDatabases()
    throws Exception
  {
    final List<Map<String, Object>> dbRows = _shell.query(
      "SELECT name FROM sys.databases WHERE name NOT IN ('" +
      join( "','", SYS_DATABASES.toArray( new String[ SYS_DATABASES.size() ] ) ) + "')" );

    final ArrayList<Database> dbs = new ArrayList<>();

    for ( final Map<String, Object> row : dbRows )
    {
      dbs.add( new Database( (String) row.get( "name" ), false, null, null, null ) );
    }
    return dbs;
  }

  public boolean databaseExists( final Database db )
    throws Exception
  {
    return !_shell.query( "SELECT * FROM sys.databases WHERE name = '" + db.getName() + "'" ).isEmpty();
  }

  private void createDatabase( final Database db )
    throws Exception
  {
    log( "Creating database ", db.getName() );
    _shell.executeUpdate(
      "CREATE DATABASE [" + db.getName() + "] " + ( null != db.getCollation() ? "COLLATE " + db.getCollation() : "" ) );
  }

  private void alterDatabase( final Database db, final ServerConfig config )
    throws Exception
  {
    // Update Collation model if needed
    if ( null != db.getCollation() )
    {
      if ( 0 == _shell.query( "SELECT name FROM sys.databases WHERE name = '" +
                              db.getName() +
                              "' and collation_name = '" +
                              db.getCollation() +
                              "'" ).size() )
      {
        log( "Updating database ", db.getName() + " collation to " + db.getCollation() );
        _shell.executeUpdate( "ALTER DATABASE [" + db.getName() + "] COLLATE " + db.getCollation() );
      }
    }

    // Update Recovery model if needed
    if ( null != db.getRecoveryModel() )
    {
      if ( 0 == _shell.query( "SELECT name FROM sys.databases WHERE name = '" +
                              db.getName() +
                              "' and recovery_model_desc = '" +
                              db.getRecoveryModel() +
                              "'" ).size() )
      {
        log( "Updating database ", db.getName() + " recovery model to " + db.getRecoveryModel() );
        _shell.executeUpdate(
          "ALTER DATABASE [" + db.getName() + "] SET RECOVERY " + db.getRecoveryModel() + " WITH NO_WAIT" );
      }
    }

    // Create users
    if ( null != db.getUsers() )
    {
      for ( final User user : db.getUsers() )
      {
        if ( !userExists( db, user ) )
        {
          createUser( db, user );
        }
        updateUser( db, user );
      }
    }

    removeUnwantedUsers( db, config );
    removeUnwantedDatabaseRoles( db, config );
    removeUnwantedPermissions( db, config );
  }

  private void createUser( final Database db, final User user )
    throws Exception
  {
    log( "Creating user ", user.getName() );

    _shell.executeUpdate(
      "USE [" + db.getName() + "]; CREATE USER [" + user.getName() + "] FOR LOGIN [" + user.getLogin() + "]" );
  }

  private void updateUser( final Database db, final User user )
    throws Exception
  {
    if ( _shell.query( "SELECT 1 FROM [" + db.getName() + "].sys.database_principals U " +
                       "JOIN sys.server_principals SP ON SP.sid = U.sid AND SP.is_disabled = 0 " +
                       "WHERE U.name = '" + user.getName() + "' AND SP.name = '" + user.getLogin() + "'" ).size() == 0 )
    {
      log( "Changing " + user.getName() + " to login " + user.getLogin() );

      _shell.executeUpdate(
        "USE [" + db.getName() + "]; ALTER USER [" + user.getName() + "] WITH LOGIN = [" + user.getLogin() + "]" );
    }

    // Grant roles
    if ( null != user.getRoles() )
    {
      for ( final String role : user.getRoles() )
      {
        ensureUserRole( db, user, role );
      }
    }

    // Process permissions
    if ( null != user.getPermissions() )
    {
      for ( final Permission permission : user.getPermissions() )
      {
        ensurePermission( db, user, permission );
      }
    }
  }

  private void removeUnwantedUsers( final Database db, final ServerConfig config )
    throws Exception
  {
    if ( !Boolean.TRUE.equals( db.isManaged() ) && !Boolean.TRUE.equals( config.isDeleteUnmanagedUsers() ) )
    {
      return;
    }

    // Obtain list of users in the database
    final List<Map<String, Object>>
      existingUsers = _shell.query( "USE [" + db.getName() + "]; " + allDatabaseUsersSQL() );

    final Set<String> wantedUsers = new HashSet<>();
    if ( null != db.getUsers() )
    {
      for ( final User user : db.getUsers() )
      {
        wantedUsers.add( user.getName().toLowerCase() );
      }
    }

    for ( final Map<String, Object> existingUserRow : existingUsers )
    {
      final String userName = (String) existingUserRow.get( "user" );

      if ( !wantedUsers.contains( userName.toLowerCase() ) )
      {
        removeUser( db, userName );
      }
    }
  }

  private void removeUser( final Database db, final String userName )
    throws Exception
  {
    log( "Dropping user " + db.getName() + "::" + userName );
    _shell.execute( "USE [" + db.getName() + "]; DROP USER [" + userName + "]" );
  }

  private void removeUnwantedDatabaseRoles( final Database db, final ServerConfig config )
    throws Exception
  {
    if ( !Boolean.TRUE.equals( db.isManaged() ) && !Boolean.TRUE.equals( config.isDeleteUnmanagedDatabaseRoles() ) )
    {
      return;
    }

    final List<Map<String, Object>> existingRoles =
      _shell.query( "USE [" + db.getName() + "]; " + allDatabaseRolesSQL() );

    // Collect all permissions that should exist and store against user name
    final Map<String, List<String>> wantedRoles = new HashMap<>();
    if ( null != db.getUsers() )
    {
      for ( final User user : db.getUsers() )
      {
        final List<String> userRoles = user.getRoles();
        if ( null == userRoles || userRoles.size() == 0 )
        {
          continue;
        }
        wantedRoles.put( user.getName().toLowerCase(), userRoles );
      }
    }

    for ( final Map<String, Object> existingRoleRow : existingRoles )
    {
      final String userName = (String) existingRoleRow.get( "user" );
      final String roleName = (String) existingRoleRow.get( "role" );

      if ( !wantedRoles.containsKey( userName ) || !wantedRoles.get( userName ).contains( roleName ) )
      {
        removeUserRole( db, userName, roleName );
      }
    }
  }

  private void removeUnwantedPermissions( final Database db, final ServerConfig config )
    throws Exception
  {
    if ( !Boolean.TRUE.equals( db.isManaged() ) && !Boolean.TRUE.equals( config.isDeleteUnmanagedPermissions() ) )
    {
      return;
    }

    // Obtain all permissions, for all users, in the database
    final List<Map<String, Object>> existingPermissions =
      _shell.query( "USE [" + db.getName() + "]; " + allDatabasePermissionsSQL() );

    // Collect all permissions that should exist and store against user name
    final Map<String, List<Permission>> wantedPermissions = new HashMap<>();
    if ( null != db.getUsers() )
    {
      for ( final User user : db.getUsers() )
      {
        final List<Permission> userPermissions = user.getPermissions();
        if ( null == userPermissions || userPermissions.size() == 0 )
        {
          continue;
        }
        wantedPermissions.put( user.getName().toLowerCase(), userPermissions );
      }
    }

    for ( final Map<String, Object> existingPermissionRow : existingPermissions )
    {
      final String userName = (String) existingPermissionRow.get( "user" );
      final String state = (String) existingPermissionRow.get( "state" );
      final String type = (String) existingPermissionRow.get( "type" );
      final String permissionName = (String) existingPermissionRow.get( "permission" );
      final Permission existingPermission =
        new Permission(
          PermissionAction.valueOf( state ),
          PermissionSecurableType.valueOf( "OBJECT_OR_COLUMN".equalsIgnoreCase( type ) ? "OBJECT" : type ),
          (String) existingPermissionRow.get( "object_name" ),
          PermissionPermission.valueOf( permissionName.replaceAll( " ", "_" ) ) );

      if ( !wantedPermissions.containsKey( userName.toLowerCase() ) )
      {
        revokePermission( db, userName, existingPermission );
      }
      else
      {
        boolean keep = false;

        for ( final Permission permission : wantedPermissions.get( userName ) )
        {
          if ( permissionsAreEqual( existingPermission, permission ) )
          {
            keep = true;
            break;
          }
        }
        if ( !keep )
        {
          revokePermission( db, userName, existingPermission );
        }
      }
    }
  }

  private boolean permissionsAreEqual( final Permission p1, final Permission p2 )
  {
    if ( !p1.getSecurableType().equals( p2.getSecurableType() ) )
    {
      return false;
    }

    if ( !p1.getPermission().equals( p2.getPermission() ) )
    {
      return false;
    }

    if ( null != p1.getAction() )
    {
      if ( p2.getAction() == null )
      {
        if ( !PermissionAction.GRANT.equals( p1.getAction() ) )
        {
          return false;
        }
      }
      else
      {
        if ( !p1.getAction().equals( p2.getAction() ) )
        {
          return false;
        }
      }
    }
    else
    {
      if ( p2.getAction() != null )
      {
        if ( !PermissionAction.GRANT.equals( p2.getAction() ) )
        {
          return false;
        }
      }
    }

    if ( null != p1.getSecurable() )
    {
      return p1.getSecurable().equalsIgnoreCase( p2.getSecurable() );
    }
    return p2.getSecurable() == null;
  }

  public void dropDatabase( final Database db )
    throws Exception
  {
    log( "Dropping database ", db.getName() );

    _shell.executeUpdate( "DROP DATABASE [" + db.getName() + "] " );
  }

  public boolean userExists( final Database database, final User user )
    throws Exception
  {
    return !_shell.query( "SELECT * FROM [" + database.getName() + "].sys.sysusers WHERE name = '" +
                          user.getName() + "'" ).isEmpty();
  }

  private void ensureUserRole( final Database db, final User user, final String role )
    throws Exception
  {
    final String hasRole = userHasRoleSQL( db.getName(), user.getName(), role );

    if ( 0 == _shell.query( hasRole ).size() )
    {
      log( "Granting role " + role + " for user " + user.getName() + " in database " + db.getName() );

      _shell.executeUpdate(
        "USE [" + db.getName() + "]; EXEC sys.sp_addrolemember [" + role + "], [" + user.getName() + "]" );
    }
  }

  private void removeUserRole( final Database db, final String user, final String role )
    throws Exception
  {
    log( "Removing role " + role + " from user " + user + " in database " + db.getName() );

    _shell.executeUpdate(
      "USE [" + db.getName() + "]; EXEC sys.sp_droprolemember [" + role + "], [" + user + "]" );
  }

  private void ensurePermission( final Database db, final User user, final Permission permission )
    throws Exception
  {
    final PermissionAction action;
    if ( null == permission.getAction() )
    {
      action = PermissionAction.GRANT;
    }
    else
    {
      action = permission.getAction();
    }

    final boolean userHasPermission =
      1 == _shell.query( userHasPermissionSQL( db.getName(), user.getName(), permission, action ) ).size();

    if ( action == PermissionAction.REVOKE && userHasPermission )
    {
      revokePermission( db, user.getName(), permission );
    }
    else if ( !userHasPermission )
    {
      log( "Adding " + action + " permission in " + db.getName() + " for " + user.getName() + ": " +
           permission.getSecurableType() + "." + permission.getPermission() + " on " + permission.getSecurable() );
      _shell.execute(
        "USE [" + db.getName() + "]; " + permissionSql( action.toString(), permission, db, user.getName() ) );
    }
    else
    {
      log( "User already has permission: " +
           permission.getAction() + " " +
           permission.getPermission() + " on " +
           permission.getSecurableType() + " for " +
           permission.getSecurable() );
    }
  }

  private void revokePermission( final Database db, final String userName, final Permission permission )
    throws Exception
  {
    log( "Revoking permission in " + db.getName() + " for " + userName + ": " +
         permission.getSecurableType() + "." + permission.getPermission() + " on " + permission.getSecurable() );
    _shell.execute( "USE [" + db.getName() + "]; " + permissionSql( "REVOKE", permission, db, userName ) );
  }

  private String permissionSql( final String action,
                                final Permission permission,
                                final Database db,
                                final String userName )
  {
    return action + " " +
           permission.getPermission().toString().replaceAll( "_", " " ) + " ON " +
           permission.getSecurableType() + "::" +
           quotedSecurable( db.getName(), permission ) + " TO [" +
           userName + "]";
  }

  protected String userHasRoleSQL( final String database, final String user, final String role )
  {
    return userRolesSQL( database, user ) + " AND R.name = '" + role + "'";
  }

  private String userRolesSQL( final String database, final String user )
  {
    return "SELECT U.name AS [user],R.name AS [role] " +
           "FROM [" + database + "].sys.database_principals R " +
           "JOIN [" + database + "].sys.database_role_members RM ON RM.role_principal_id = R.principal_id " +
           "JOIN [" + database + "].sys.database_principals U ON RM.member_principal_id = U.principal_id " +
           "WHERE R.is_fixed_role = 1 AND " +
           "U.name = '" + user + "'";
  }

  private String[] resolvedSecurable( final String database, final Permission permission )
  {
    if ( null == permission )
    {
      return new String[]{ database, database };
    }

    if ( permission.getSecurableType() == PermissionSecurableType.DATABASE )
    {
      if ( null != permission.getSecurable() )
      {
        throw new RuntimeException( "Must not specify securable if securable_type is DATABASE" );
      }
      return new String[]{ database, database };
    }

    if ( null == permission.getSecurable() )
    {
      throw new RuntimeException( "Must specify securable if securable_type is not DATABASE" );
    }
    final String[] result = permission.getSecurable().replaceAll( "\\[", "" ).replaceAll( "\\]", "" ).split( "\\." );
    if ( result.length == 1 )
    {
      return new String[]{ "dbo", result[ 0 ] };
    }
    return result;
  }

  private String securableSchema( final String database, final Permission permission )
  {
    return resolvedSecurable( database, permission )[ 0 ];
  }

  private String securableName( final String database, final Permission permission )
  {
    return resolvedSecurable( database, permission )[ 1 ];
  }

  private String quotedSecurable( final String database, final Permission permission )
  {
    if ( PermissionSecurableType.DATABASE == permission.getSecurableType() )
    {
      return "[" + database + "]";
    }
    return "[" + securableSchema( database, permission ) + "].[" + securableName( database, permission ) + "]";
  }

  private String securableTypeClassDesc( final Permission permission )
  {
    return permission.getSecurableType() == PermissionSecurableType.OBJECT ?
           "OBJECT_OR_COLUMN" : permission.getSecurableType().toString().replaceAll( "_", " " );
  }

  protected String userHasPermissionSQL( final String database,
                                         final String user,
                                         final Permission permission,
                                         final PermissionAction state )
  {
    return userPermissionsSQL( database, user, permission ) +
           " AND P.class_desc = '" + securableTypeClassDesc( permission ) + "'" +
           " AND P.permission_name = '" + permission.getPermission().toString().replaceAll( "_", " " ) + "'" +
           ( state != PermissionAction.REVOKE ? " AND P.state_desc = '" + state + "'" : "" ) +
           " AND COALESCE(O.name,T.name,'" + database + "') = '" + securableName( database, permission ) + "'";
  }

  protected String userPermissionsSQL( final String database, final String user, final Permission permission )
  {
    return
      "SELECT U.name as [user], P.class_desc as [type], P.permission_name as [permission], P.state_desc as [state], O.name as [object_name], S.name as [schema] " +
      "FROM sys.database_permissions P " +
      "JOIN sys.database_principals U ON P.grantee_principal_id = U.principal_id AND U.type_desc IN ('SQL_USER','WINDOWS_USER','WINDOWS_GROUP')" +
      "LEFT JOIN sys.all_objects O ON O.object_id = P.major_id " +
      "LEFT JOIN sys.types T ON T.user_type_id = P.major_id " +
      "LEFT JOIN sys.schemas S ON S.schema_id = COALESCE(O.schema_id,T.schema_id) " +
      "WHERE " +
      "U.name = '" +
      user +
      "' AND COALESCE(S.name,'" +
      database +
      "') = '" +
      securableSchema( database, permission ) +
      "'";
  }

  private String allServerRolesSQL()
  {
    return "SELECT P.name as login, RP.name as role " +
           "FROM " +
           "sys.server_principals P " +
           "JOIN sys.server_role_members SRM ON SRM.member_principal_id = P.principal_id " +
           "JOIN sys.server_principals RP ON RP.principal_id = SRM.role_principal_id AND RP.type_desc = 'SERVER_ROLE' " +
           "WHERE " +
           "P.is_disabled = 0 AND " +
           "P.name NOT LIKE 'NT AUTHORITY\\\\%' AND " +
           "P.name NOT LIKE 'NT SERVICE\\\\%' AND " +
           "P.type_desc IN ('SQL_LOGIN', 'WINDOWS_GROUP', 'WINDOWS_LOGIN')";
  }

  private String allDatabasePermissionsSQL()
  {
    return
      "SELECT U.name as [user], P.class_desc as [type], P.permission_name as [permission], P.state_desc as [state], O.name as [object_name], S.name as [schema] " +
      "FROM sys.database_permissions P " +
      "JOIN sys.database_principals U ON P.grantee_principal_id = U.principal_id AND U.type_desc IN ('SQL_USER','WINDOWS_USER','WINDOWS_GROUP')" +
      "LEFT JOIN sys.all_objects O ON O.object_id = P.major_id " +
      "LEFT JOIN sys.types T ON T.user_type_id = P.major_id " +
      "LEFT JOIN sys.schemas S ON S.schema_id = COALESCE(O.schema_id,T.schema_id) " +
      "WHERE P.class_desc IN ('DATABASE','TYPE','OBJECT_OR_COLUMN') " +
      "AND U.name != 'dbo'";
  }

  private String allDatabaseRolesSQL()
  {
    return
      "SELECT U.name AS [user], R.name AS [role] " +
      "FROM " +
      "sys.database_principals R " +
      "JOIN sys.database_role_members RM ON RM.role_principal_id = R.principal_id " +
      "JOIN sys.database_principals U ON RM.member_principal_id = U.principal_id " +
      "WHERE " +
      "R.is_fixed_role = 1 AND " +
      "U.name != 'dbo'";
  }

  private String allDatabaseUsersSQL()
  {
    return "SELECT U.name AS [user] " +
           "FROM sys.database_principals U " +
           "JOIN sys.server_principals SP ON SP.sid = U.sid AND SP.is_disabled = 0 AND SP.name NOT LIKE 'NT AUTHORITY%' AND SP.name NOT LIKE 'NT SERVICE%' " +
           "WHERE " +
           "U.name != 'dbo' AND " +
           "U.type_desc IN ('SQL_USER','WINDOWS_USER','WINDOWS_GROUP')";
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
