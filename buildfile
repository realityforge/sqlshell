require 'buildr/git_auto_version'

COMPILE_DEPS = [:getopt4j, :json, :javax_jsr305]
TEST_DEPS = [:jtds, :postgresql]

desc 'sqlshell: Command line shell to interact with a database'
define 'sqlshell' do
  project.group = 'org.realityforge.sqlshell'
  compile.options.source = '1.7'
  compile.options.target = '1.7'
  compile.options.lint = 'all'

  pom.add_apache2_license
  pom.add_github_project('realityforge/sqlshell')
  pom.add_developer('realityforge', 'Peter Donald', 'peter@realityforge.org', ['Developer'])

  compile.with COMPILE_DEPS, _(:generated, :main, :domgen, :main, :java)

  test.using :testng
  test.with TEST_DEPS

  package(:jar)
  package(:sources)
  package(:javadoc)
  package(:jar, :classifier => 'all').tap do |jar|
    jar.merge(artifact(:getopt4j))
    jar.merge(artifact(:json))
  end

  Domgen::GenerateTask.new(:sqlshell,
                           :ee,
                           [:ee_data_types],
                           _(:generated, 'main/domgen'))

  project.clean {rm_rf _(:generated)}
end
