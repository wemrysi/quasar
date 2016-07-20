import precog.PlatformBuild._

def scalazVersion = "7.2.4"

lazy val root = project.setup.root.noArtifacts aggregate (blueeyes, yggdrasil) dependsOn (yggdrasil % BothScopes) also (
  initialCommands in console := "import blueeyes._, json._"
)

/** mimir used to be the evaluator project.
 */
lazy val mimir     = project.setup.noArtifacts dependsOn yggdrasil % BothScopes
lazy val yggdrasil = project.setup dependsOn blueeyes % BothScopes

lazy val blueeyes = (
  project.setup deps (
    "org.joda"           % "joda-convert"      %    "1.8.1",
    "joda-time"          % "joda-time"         %    "2.9.4",
    "com.chuusai"       %% "shapeless"         %    "2.3.1",
    "org.slf4s"         %% "slf4s-api"         %   "1.7.13",
    "org.spire-math"    %% "spire"             %    "0.7.4",
    "org.scalaz"        %% "scalaz-effect"     % scalazVersion,
    "org.scalaz"        %% "scalaz-concurrent" % scalazVersion,
    "org.scalaz.stream" %% "scalaz-stream"     %    "0.8.1a"    % Test,
    "org.specs2"        %% "specs2-scalacheck" %     "3.7"      % Test,
    "org.specs2"        %% "specs2-core"       %     "3.7"      % Test
  )
)

lazy val all = project.setup.root.noArtifacts aggregate (blueeyes, yggdrasil, mimir) dependsOn (mimir % BothScopes)
