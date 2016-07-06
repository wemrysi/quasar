import precog.PlatformBuild._

lazy val platform = project.root aggregate (util, common, bytecode, yggdrasil, mimir)
lazy val logging  = project.setup  // common project for the test log configuration files
lazy val util     = project.setup.testLogging deps (

  "joda-time"              % "joda-time"        % "1.6.2",
  "javolution"             % "javolution"       % "5.5.1",
  "org.xlightweb"          % "xlightweb"        % "2.13.2",
  "com.google.guava"       % "guava"            %  "12.0",
  "commons-io"             % "commons-io"       %  "2.5",
  "com.rubiconproject.oss" % "jchronic"         % "0.2.6",
  "javax.mail"             % "mail"             % "1.4.7",
  "org.fusesource.scalate" % "scalate-core_2.9" %  "1.6.1"
)
lazy val common   = project.setup.testLogging dependsOn util deps (
  "com.chuusai" %% "shapeless" % "1.2.3"
)
lazy val bytecode = project.setup.testLogging also (
  publishArtifact in packageDoc := false
)
lazy val yggdrasil = project.assemblyProject.usesCommon dependsOn (bytecode, niflheim) deps (

  "com.reportgrid"       %% "blueeyes-mongo"     % blueeyesVersion,
  "org.objectweb.howl"    % "howl"               %    "1.0.1-1",
  "org.slamdata"          % "jdbm"               %     "3.0.0",
  "commons-primitives"    % "commons-primitives" %      "1.0",
  "org.quartz-scheduler"  % "quartz"             %     "2.2.3",
  "org.spire-math"        % "spire_2.9.2"        %     "0.3.0",
  "com.typesafe.akka"     % "akka-testkit"       %     "2.0.5"      % "test"
)

lazy val blueeyes = project also commonSettings deps (
  "com.reportgrid" %% "blueeyes-json" % blueeyesVersion,
  "com.reportgrid" %% "blueeyes-core" % blueeyesVersion,
  "com.reportgrid" %% "akka_testing"  % blueeyesVersion excludeAll(
    ExclusionRule(organization = "com.reportgrid")
  )
)
/** This used to be the evaluator project.
 */
lazy val mimir = project.setup.usesCommon.testLogging dependsOn (util.inBothScopes, bytecode.inBothScopes, yggdrasil.inBothScopes)
