require 'buildr/git_auto_version'

COMPILE_DEPS = [:spice_cli, :json]
TEST_DEPS = [:jtds, :postgresql]

desc 'sqlshell: Command line shell to interact with a database'
define 'sqlshell' do
  project.group = 'org.realityforge.sqlshell'
  compile.options.source = '1.6'
  compile.options.target = '1.6'
  compile.options.lint = 'all'

  compile.with COMPILE_DEPS

  test.using :testng
  test.with TEST_DEPS

  package(:jar)
  package(:jar, :classifier => 'all').tap do |jar|
    jar.merge(artifact(:spice_cli))
    jar.merge(artifact(:json))
  end
end
