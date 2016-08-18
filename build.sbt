import yggbuild.Build._

def scalazVersion     = "7.2.4"
def specsVersion      = "3.8.4-scalacheck-1.12.5"
def scalacheckVersion = "1.12.5"
def circeVersion      = "0.4.1"

def repl = """
  |import quasar._, precog._, blueeyes._, json._
  |import com.precog._, common._, ygg._
  |import io.circe._, literal._
""".stripMargin.trim

lazy val root = project.root.setup.noArtifacts aggregate (precog, ygg) dependsOn (precog, ygg) also (
  initialCommands in console := repl
)

lazy val ygg = project.setup dependsOn (precog % BothScopes)

lazy val precog = project.setup deps (
  "org.spire-math" %% "jawn-ast"          % "0.9.0",
  "io.circe"       %% "circe-literal"     % circeVersion,
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
