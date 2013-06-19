require 'buildr/git_auto_version'

desc 'sqlshell: Command line shell to interact with a database'
define 'sqlshell' do
  project.group = 'org.realityforge.sqlshell'
  compile.options.source = '1.6'
  compile.options.target = '1.6'
  compile.options.lint = 'all'

  compile.with :spice_cli,
               :json,
               :jtds,
               :postgresql

  test.using :testng

  package(:jar)
  package(:jar, :classifier => 'all').tap do |jar|
    jar.merge(artifact(:spice_cli))
    jar.merge(artifact(:json))
  end
end
