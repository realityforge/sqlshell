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
    end

    data_module.struct(:User) do |t|
      t.description(<<TEXT)
A User is an account within a Database that allows a Login to access artifacts within the database
TEXT

      # The name of the user within the database
      t.string(:Name, 50)

      # The name of the Login that this user is associated with
      t.string(:Login, 50)
    end

    data_module.struct(:Database) do |t|
      t.description(<<TEXT)
A Database is a database that is expected to exist on the database server
TEXT
      # The name of the database
      t.string(:Name, 50)

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
      t.boolean(:DeleteUnmanagedLogins)
      t.struct(:Login, :Login, :collection_type => :sequence)

      t.boolean(:DeleteUnmanagedDatabases)
      t.struct(:Database, :Database, :collection_type => :sequence)
    end
  end
end
