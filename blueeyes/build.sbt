import blueeyes.BlueeyesBuild._

lazy val blueeyes = project.setup.root.noArtifacts aggregate (util, json, bkka, core, test)

lazy val core = project.setup dependsOn (json, bkka, util % BothScopes) deps (

  "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" %      "1.1",
  "javolution"                             % "javolution"                  %     "5.5.1",
  "io.netty"                               % "netty"                       %  "3.6.3.Final",
  "org.streum"                             % "configrity-core_2.9.2"       %     "1.0.0",
  "org.xlightweb"                          % "xlightweb"                   %     "2.13.2",
  "javax.servlet"                          % "javax.servlet-api"           %      "3.0.1"      % "provided",
  "org.mockito"                            % "mockito-all"                 %      "1.9.0"      %    Test,
  "org.eclipse.jetty"                      % "jetty-server"                % "8.1.3.v20120416" %    Test,
  "org.eclipse.jetty"                      % "jetty-servlet"               % "8.1.3.v20120416" %    Test
)
lazy val util = project.setup deps (

  "org.slf4s"         %% "slf4s-api"     % "1.7.13",
  "org.scalaz"        %% "scalaz-effect" %  "7.0.9",
  "joda-time"          % "joda-time"     %  "1.6.2",
  "com.typesafe.akka"  % "akka-actor"    %  "2.0.5",
  "org.specs2"        %% "specs2"        % "1.12.4.1" % Test
)
lazy val json   = project.setup dependsOn (util % BothScopes) deps (
  "com.chuusai" %% "shapeless" % "1.2.3"
)

lazy val bkka = project.setup dependsOn (util % BothScopes)
lazy val test = project.setup.noArtifacts dependsOn (core, util % BothScopes)
