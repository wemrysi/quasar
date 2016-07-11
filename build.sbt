import precog.PlatformBuild._

lazy val blueeyes = ProjectRef(file("blueeyes").toURI, "core")

lazy val platform = project.setup.root.noArtifacts aggregate (blueeyes, common, yggdrasil, mimir)

/** This used to be the evaluator project.
 */
lazy val mimir = project.setup.noArtifacts dependsOn yggdrasil.inBothScopes

lazy val yggdrasil = project.setup dependsOn common.inBothScopes deps (
  "org.objectweb.howl" % "howl"        % "1.0.1-1",
  "org.slamdata"       % "jdbm"        %  "3.0.0",
  "org.spire-math"     % "spire_2.9.2" %  "0.3.0"
)
lazy val common = project.setup dependsOn blueeyes deps (

  "com.chuusai"              %% "shapeless"       %  "1.2.3",
  "org.slf4s"                %% "slf4s-api"       % "1.7.13",
  "org.scalaz"               %% "scalaz-core"     %  "7.0.9",
  "com.google.code.findbugs"  % "jsr305"          %  "3.0.1",
  "joda-time"                 % "joda-time"       %  "1.6.2",
  "com.google.guava"          % "guava"           % "12.0.1",
  "com.rubiconproject.oss"    % "jchronic"        %  "0.2.6",
  "org.scalacheck"           %% "scalacheck"      %  "1.10.1"  % Test,
  "org.specs2"               %% "specs2"          % "1.12.4.1" % Test,
  "ch.qos.logback"            % "logback-classic" %  "1.0.0"   % Test
)
