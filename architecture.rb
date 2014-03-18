Domgen.repository(:sqlshell) do |repository|
  repository.enable_facet(:ee)
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
      t.string(:Name, 50)
      t.string(:Login, 50)
    end

    data_module.struct(:Database) do |t|
      t.description(<<TEXT)
A Database is a database that is expected to exist on the database server
TEXT
      t.string(:Name, 50)

      t.struct(:User, :User, :collection_type => :sequence)
    end

    data_module.struct(:ServerConfig) do |t|
      t.description(<<TEXT)
The ServerConfig is the primary container which defines all the entire desired state of the database server
TEXT
      t.boolean(:RemoveUnwantedLogins)
      t.struct(:Login, :Login, :collection_type => :sequence)

      t.boolean(:RemoveUnwantedDatabases)
      t.struct(:Database, :Database, :collection_type => :sequence)
    end
  end
end
