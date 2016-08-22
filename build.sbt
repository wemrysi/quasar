import xygg.build.Build._

def repl = """
  |import ygg._, blueeyes._, json._
""".stripMargin.trim

lazy val root = project.root.setup.noArtifacts aggregate (macros, ygg) dependsOn (ygg % BothScopes) also (
  initialCommands in console := repl
)

lazy val macros = project.setup also macroDependencies

lazy val ygg = project.setup dependsOn (macros) deps (

  "org.spire-math" %% "jawn-parser"       % "0.9.0",
  "org.mapdb"      %  "mapdb"             % "3.0.1",
  "org.spire-math" %% "spire-macros"      % "0.11.0",
  "org.scalaz"     %% "scalaz-core"       % scalazVersion force(),
  "org.scalacheck" %% "scalacheck"        % scalacheckVersion      % Test force(),
  "org.specs2"     %% "specs2-scalacheck" % specsVersion           % Test,
  "org.typelevel"  %% "scalaz-specs2"     % "0.4.0"                % Test,
  "org.specs2"     %% "specs2-core"       % specsVersion           % Test
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
