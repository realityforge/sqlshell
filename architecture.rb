Domgen.repository(:sqlshell) do |repository|
  repository.enable_facet(:ee)
  repository.enable_facet(:jackson)
  repository.java.base_package = 'org.realityforge.sqlshell'

  repository.data_module(:MsSQL) do |data_module|
    data_module.ee.data_type_package = 'org.realityforge.sqlshell.data_type.mssql'
    data_module.struct(:Login) do |t|
      t.description(<<TEXT)
A Login is a username/pwd that is capable of connecting to the database server
TEXT
      # The name of the login
      t.string(:Name, 50)

      # The password of the account if creating creating a sql server login.
      t.string(:Password, 50, :nullable => true)

      # The default database for the login.
      t.string(:DefaultDatabase, 50, :nullable => true)

      # The default language for the login.
      t.string(:DefaultLanguage, 50, :nullable => true)

      # The list of server roles that the login is granted
      t.s_enum(:ServerRole, %w(PUBLIC SYSADMIN SECURITYADMIN SERVERADMIN SETUPADMIN PROCESSADMIN DISKADMIN DBCREATOR BULKADMIN), :collection_type => :sequence, :nullable => true)
    end

    # A Permission granted to a database user
    data_module.struct(:Permission) do |t|
      # The type of permission being applied.  Default behaviour is grant
      t.s_enum(:Action, %w(GRANT DENY REVOKE), :nullable => true)

      # The type of the thing being configured
      t.s_enum(:SecurableType, %w(DATABASE OBJECT TYPE))

      # The name of the thing being secured, if required
      t.string(:Securable, 100, :nullable => true)

      # The type of permission being applied
      t.s_enum(:Permission, [
                    'BACKUP_DATABASE', 'BACKUP_LOG', 'CREATE_DATABASE', 'CREATE_DEFAULT', 'CREATE_FUNCTION',
                    'CREATE_PROCEDURE', 'CREATE_RULE', 'CREATE_TABLE', 'CREATE_VIEW',
                    'EXECUTE', 'REFERENCES', 'DELETE', 'INSERT', 'UPDATE', 'SELECT', 'CONNECT'])
    end

    data_module.struct(:User) do |t|
      t.description(<<TEXT)
A User is an account within a Database that allows a Login to access artifacts within the database
TEXT

      # The name of the user within the database
      t.string(:Name, 50)

      # The name of the Login that this user is associated with
      t.string(:Login, 50)

      # The list of roles that the user is granted within the database
      t.string(:Role, 50, :collection_type => :sequence, :nullable => true)

      # The list of permissions to apply to the user
      t.struct(:Permission, :Permission, :collection_type => :sequence, :nullable => true)
    end

    data_module.struct(:Database) do |t|
      t.description(<<TEXT)
A Database is a database that is expected to exist on the database server
TEXT
      # The name of the database
      t.string(:Name, 50)

      # Whether this database is 'managed'.
      # Any users/roles/permissions not found in the configuration will be deleted from a Managed database
      # Default is false
      t.boolean(:Managed, :nullable => true)

      # The collation for the database.  If not specified defaults to the server default
      t.string(:Collation, 50, :nullable => true)

      # The recovery model for the database.  If not specified defaults to the server default
      t.s_enum(:RecoveryModel, ['FULL','SIMPLE'], :nullable => true)

      # The set of users to create for this database
      t.struct(:User, :User, :collection_type => :sequence, :nullable => true)


    end

    data_module.struct(:ServerConfig) do |t|
      t.description(<<TEXT)
The ServerConfig is the primary container which defines all the entire desired state of the database server
TEXT
      # The logins that must exist
      t.struct(:Login, :Login, :collection_type => :sequence)

      # The databases that must exist
      t.struct(:Database, :Database, :collection_type => :sequence)

      # Whether existing databases not listed in [Databases] should be deleted from the server
      # Default is false
      t.boolean(:DeleteUnmanagedDatabases, :nullable => true)

      # Whether existing logins not listed in [Logins] should be deleted from managed databases
      # Default is false
      t.boolean(:DeleteUnmanagedLogins, :nullable => true)

      # Whether existing roles should be deleted from Managed databases if they are not included in the configuration
      # Default is false
      t.boolean(:DeleteUnmanagedDatabaseRoles, :nullable => true)

      # Whether existing permissions should be deleted from Managed databases if they are not included in the configuration
      # Default is false
      t.boolean(:DeleteUnmanagedPermissions, :nullable => true)

      # Whether existing users should be deleted from Managed databases if they are not included in the configuration
      # Default is false
      t.boolean(:DeleteUnmanagedUsers, :nullable => true)

      # Whether existing roles should be deleted from the server if they are not included in the configuration
      # Default is false
      t.boolean(:DeleteUnmanagedServerRoles, :nullable => true)
    end
  end
end
