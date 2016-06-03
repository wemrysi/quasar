name := "blueeyes-core"

libraryDependencies ++= Seq(
  "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.1",
  "org.slf4s"                   %% "slf4s-api"          % "1.7.13",
  "commons-codec"               % "commons-codec"       % "1.5",
  "javolution"                  % "javolution"          % "5.5.1",
  "joda-time"                   % "joda-time"           % "1.6.2",
  "io.netty"                    % "netty"               % "3.6.3.Final",
  "org.streum"                  % "configrity-core_2.9.2" % "1.0.0",
  "org.xlightweb"               % "xlightweb"           % "2.13.2",
  "javax.servlet"               % "javax.servlet-api"   % "3.0.1"           % "provided",
  "org.mockito"                 % "mockito-all"         % "1.9.0"           % "test",
  "org.eclipse.jetty"           % "jetty-server"        % "8.1.3.v20120416" % "test",
  "org.eclipse.jetty"           % "jetty-servlet"       % "8.1.3.v20120416" % "test"
)

ivyXML :=
<dependencies>
  <dependency org="org.eclipse.jetty" name="jetty-server" rev="8.1.3.v20120416" conf="test">
    <exclude org="org.eclipse.jetty.orbit" />
  </dependency>
</dependencies>

parallelExecution in Test := false


