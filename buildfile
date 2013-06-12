require 'buildr/git_auto_version'

desc 'sqlcli: Command line shell to interact with a database'
define 'sqlcli' do
  project.group = 'org.realityforge.sqlcli'
  compile.options.source = '1.6'
  compile.options.target = '1.6'
  compile.options.lint = 'all'

  compile.with :spice_cli,
               :jtds,
               :postgresql

  test.using :testng

  package(:jar)
  package(:jar, :classifier => 'all').tap do |jar|
    jar.merge(artifact(:spice_cli))
  end
end
