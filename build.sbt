import xygg._, YggBuild._

lazy val root = project.root.setup.noArtifacts aggregate (macros, ygg) dependsOn (ygg % BothScopes) also (
  initialCommands in console := "import ygg._, common._, json._, data._, table._"
)

lazy val macros = project.setup also macroDependencies

lazy val ygg = project.setup dependsOn macros deps (yggDependencies: _*) also (
  sourceGenerators in Compile += Boilerplate.generate.taskValue
)

lazy val benchmark = project.setup dependsOn (ygg % BothScopes) enablePlugins JmhPlugin also (
                fork in Test :=  true,
      sourceDirectory in Jmh <<= sourceDirectory in Test,
       classDirectory in Jmh <<= classDirectory in Test,
  dependencyClasspath in Jmh <<= dependencyClasspath in Test,
              compile in Jmh <<= (compile in Jmh) dependsOn (compile in Test),
                  run in Jmh <<= (run in Jmh) dependsOn (Keys.compile in Jmh)
)

addCommandAlias("bench", "benchmark/jmh:run -f1 -t1")
addCommandAlias("cc", "test:compile")
addCommandAlias("tt", "test")
addCommandAlias("ttq", "testQuick")
addCommandAlias("tcon", "test:console")
addCommandAlias("tconq", "test:consoleQuick")
addCommandAlias("cover", "; coverage ; test ; coverageReport")
