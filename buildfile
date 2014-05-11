require 'buildr/git_auto_version'

COMPILE_DEPS = [:getopt4j, :json]
TEST_DEPS = [:jtds, :postgresql]

desc 'sqlshell: Command line shell to interact with a database'
define 'sqlshell' do
  project.group = 'org.realityforge.sqlshell'
  compile.options.source = '1.6'
  compile.options.target = '1.6'
  compile.options.lint = 'all'

  pom.add_apache_v2_license
  pom.add_github_project('realityforge/sqlshell')
  pom.add_developer('realityforge', 'Peter Donald', 'peter@realityforge.org', ['Developer'])

  compile.with COMPILE_DEPS

  test.using :testng
  test.with TEST_DEPS

  package(:jar)
  package(:sources)
  package(:javadoc)
  package(:jar, :classifier => 'all').tap do |jar|
    jar.merge(artifact(:getopt4j))
    jar.merge(artifact(:json))
  end
end
