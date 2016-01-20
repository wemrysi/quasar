/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.api

import quasar.Predef._
import quasar._
import quasar.api.services.RestApi
import quasar.console._
import quasar.config._
import quasar.fp._
import quasar.fs.{FileSystem, InMemory}
import quasar.fs.mount._

import java.io.File
import java.lang.System

import scala.concurrent.duration._

import argonaut.CodecJson
import org.http4s.server.{Server => Http4sServer, HttpService}
import org.http4s.server.blaze.BlazeBuilder
import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._
import shapeless.{Coproduct => _, _}, nat._, ops.nat._

object ServerOps {
  type Builders = List[(Int, BlazeBuilder)]
  type Servers = List[(Int, Http4sServer)]
  type ServersErrors = List[(Int, Throwable \/ Http4sServer)]
  type Services = List[(String, HttpService)]

  final case class Options(
    config: Option[String],
    contentLoc: Option[String],
    contentPath: Option[String],
    contentPathRelative: Boolean,
    openClient: Boolean,
    port: Option[Int])

  final case class StaticContent(loc: String, path: String)
}

abstract class ServerOps[WC: CodecJson, SC](
    configOps: ConfigOps[WC],
    defaultWC: WC,
    val webConfigLens: WebConfigLens[WC, SC]) {
  import ServerOps._
  import webConfigLens._

  type MainErrT[F[_], A] = EitherT[F, String, A]
  type MainTask[A]       = MainErrT[Task, A]

  val mainTask = MonadError[EitherT[Task,?,?], String]

  // NB: This is a terrible thing.
  //     Is there a better way to find the path to a jar?
  val jarPath: Task[String] =
    Task.delay {
      val uri = getClass.getProtectionDomain.getCodeSource.getLocation.toURI
      val path0 = uri.getPath
      val path =
        java.net.URLDecoder.decode(
          Option(uri.getPath)
            .getOrElse(uri.toURL.openConnection.asInstanceOf[java.net.JarURLConnection].getJarFileURL.getPath),
          "UTF-8")
      (new java.io.File(path)).getParentFile().getPath() + "/"
    }

  /** Returns why the given port is unavailable or None if it is available. */
  def unavailableReason(port: Int): OptionT[Task, String] =
    OptionT(Task.delay(new java.net.ServerSocket(port)).attempt.flatMap {
      case -\/(err: java.net.BindException) => Task.now(Some(err.getMessage))
      case -\/(err)                         => Task.fail(err)
      case \/-(s)                           => Task.delay(s.close()).as(None)
    })

  /** An available port number. */
  def anyAvailablePort: Task[Int] = anyAvailablePorts[_1].map(_.head)

  /** Available port numbers. */
  def anyAvailablePorts[A <: Nat: ToInt]: Task[Sized[IndexedSeq[Int], A]] = Task.delay {
    Sized.wrap(
      (1 to toInt[A])
        .map(_ => { val s = new java.net.ServerSocket(0); (s, s.getLocalPort) })
        .map { case (s, p) => { s.close; p } })
  }

  /** Returns the requested port if available, or the next available port. */
  def choosePort(requested: Int): Task[Int] =
    unavailableReason(requested)
      .flatMapF(rsn => stderr("Requested port not available: " + requested + "; " + rsn) *>
                       anyAvailablePort)
      .getOrElse(requested)

  case class ServerBlueprint(port: Int, idleTimeout: Duration, svcs: ListMap[String, HttpService])

  /** Start `Server` with supplied [[ServerBlueprint]]
    * @param flexibleOnPort Whether or not to choose an alternative port if requested port is not available
    * @return Server that has been started along with the port on which it was started
    */
  def startServer(blueprint: ServerBlueprint, flexibleOnPort: Boolean): Task[(Http4sServer, Int)] = {
    for {
      actualPort <- if (flexibleOnPort) choosePort(blueprint.port) else Task.now(blueprint.port)
      builder <- Task.delay {
        val initialBuilder = BlazeBuilder
          .withIdleTimeout(blueprint.idleTimeout)
          .bindHttp(actualPort, "0.0.0.0")

        blueprint.svcs.toList.reverse.foldLeft(initialBuilder) {
          case (b, (path, svc)) => b.mountService(Prefix(path)(svc))
        }
      }
      server <- Task.delay(builder.run)
    } yield (server, actualPort)
  }

  /** Given a `Process` of [[ServerBlueprint]], returns a `Process` of `Server`.
    *
    * The returned process will emit each time a new server configuration is provided and ensures only
    * one server is running at a time, i.e. providing a new Configuration ensures
    * the previous server has been stopped.
    *
    * When the process of configurations terminates for any reason, the last server is shutdown and the
    * process of servers will terminate.
    * @param flexibleOnPort Whether or not to choose an alternative port if requested port is not available
    */
  def servers(configurations: Process[Task, ServerBlueprint], flexibleOnPort: Boolean): Process[Task, (Http4sServer,Int)] = {

    val serversAndPort = configurations.evalMap(conf =>
      startServer(conf, flexibleOnPort).onSuccess { case (_, port) =>
        stdout("Server started listening on port " + port) })

    serversAndPort.evalScan1 { case ((oldServer, oldPort), newServerAndPort) =>
      oldServer.shutdown.flatMap(_ => stdout("Stopped server listening on port " + oldPort)) *>
      Task.now(newServerAndPort)
    }.cleanUpWithA{ server =>
      server.map { case (lastServer, lastPort) =>
        lastServer.shutdown.flatMap(_ => stdout("Stopped last server listening on port " + lastPort))
      }.getOrElse(Task.now(()))
    }
  }

  /** Produce a stream of servers that can be restarted on a supplied port
    * @param initialPort The port on which to start the initial server
    * @param produceRoutes A function that given a function to restart a server on a new port, supplies a server mapping
    *                      from path to `Server`
    * @return The `Task` will start the first server and provide a function to shutdown the active server.
    *         It will also return a process of servers and ports. This `Process` must be run in order for servers
    *         to actually be started and stopped. The `Process` must be run to completion in order for appropriate
    *         clean up to occur.
    */
  def startServers(initialPort: Int,
                   produceRoutes: (Int => Task[Unit]) => ListMap[String, HttpService]): Task[(Process[Task, (Http4sServer,Int)], Task[Unit])] = {
    val configQ = async.boundedQueue[ServerBlueprint](1)
    def startNew(port: Int): Task[Unit] = {
      val conf = ServerBlueprint(port, idleTimeout = Duration.Inf, produceRoutes(startNew))
      configQ.enqueueOne(conf)
    }
    startNew(initialPort).flatMap(_ => servers(configQ.dequeue, false).unconsOption.map {
      case None => throw new java.lang.AssertionError("should never happen")
      case Some((head, rest)) => (Process.emit(head) ++ rest, configQ.close)
    })
  }

  // Lifted from unfiltered.
  // NB: available() returns 0 when the stream is closed, meaning the server
  //     will run indefinitely when started from a script.
  private def waitForInput: Task[Unit] = {
    import java.lang.System
    for {
      _    <- Task.delay(java.lang.Thread.sleep(250))
                  .handle { case _: java.lang.InterruptedException => () }
      test <- Task.delay(Option(System.console).isEmpty || System.in.available() <= 0)
                  .handle { case _ => true }
      done <- if (test) waitForInput else Task.now(())
    } yield done
  }

  private def openBrowser(port: Int): Task[Unit] = {
    val url = "http://localhost:" + port + "/"
    Task.delay(java.awt.Desktop.getDesktop().browse(java.net.URI.create(url)))
        .or(stderr("Failed to open browser, please navigate to " + url))
  }

  // scopt's recommended OptionParser construction involves side effects
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  val optionParser = new scopt.OptionParser[Options]("quasar") {
    head("quasar")
    opt[String]('c', "config") action { (x, c) => c.copy(config = Some(x)) } text("path to the config file to use")
    opt[String]('L', "content-location") action { (x, c) => c.copy(contentLoc = Some(x)) } text("location where static content is hosted")
    opt[String]('C', "content-path") action { (x, c) => c.copy(contentPath = Some(x)) } text("path where static content lives")
    opt[Unit]('r', "content-path-relative") action { (_, c) => c.copy(contentPathRelative = true) } text("specifies that the content-path is relative to the install directory (not the current dir)")
    opt[Unit]('o', "open-client") action { (_, c) => c.copy(openClient = true) } text("opens a browser window to the client on startup")
    opt[Int]('p', "port") action { (x, c) => c.copy(port = Some(x)) } text("the port to run Quasar on")
    help("help") text("prints this usage text")
  }

  def interpretPaths(options: Options): MainTask[Option[StaticContent]] = {
    val defaultLoc = "/files"

    def path(p: String): Task[String] =
      if (options.contentPathRelative) jarPath.map(_ + p)
      else p.point[Task]

    (options.contentLoc, options.contentPath) match {
      case (None, None) =>
        none.point[MainTask]
      case (Some(_), None) =>
        mainTask.raiseError("content-location specified but not content-path")
      case (loc, Some(p)) =>
        path(p).map(p => some(StaticContent(loc.getOrElse(defaultLoc), p))).liftM[MainErrT]
    }
  }

  type MountingPlusFileSystem[A] = Coproduct[MountingF, FileSystem, A]

  def interpreter(config: WC): Task[MountingPlusFileSystem ~> Task] = {
    // TODO: use the real mount and hierarchical interpreters
    import InMemory._

    val mount = new (Mounting ~> Task) {
      def apply[A](m: Mounting[A]): Task[A] = Task.fail(new RuntimeException("TODO"))
    }

    runFs(InMemState.empty).map(fs => free.interpret2[MountingF, FileSystem, Task](Coyoneda.liftTF(mount), fs))
  }

  def main(args: Array[String]): Unit = {
    val exec: MainTask[Unit] = for {
      opts          <- optionParser.parse(args, Options(None, None, None, false, false, None))
                         .cata(_.point[MainTask], mainTask.raiseError("couldn't parse options"))
      content       <- interpretPaths(opts)
      redirect      =  content.map(_.loc)
      cfgPath       <- opts.config.fold(none[FsFile].point[MainTask])(cfg =>
                         FsPath.parseSystemFile(cfg)
                           .toRight(s"Invalid path to config file: $cfg")
                           .map(some))
      config        <- configOps.fromFileOrDefaultPaths(cfgPath)
                         .leftMap(_.shows)
                         .fixedOrElse(defaultWC.point[MainTask])
      port          =  opts.port getOrElse wcPort.get(config)
      updCfg        =  wcPort.set(port)(config)
      fs            <- interpreter(config).liftM[MainErrT]
      produceRoutes =  (reload: (Int => Task[Unit])) =>
                         RestApi(content.toList, redirect, port, reload).AllServices(fs)
      result        <- startServers(port, produceRoutes).liftM[MainErrT]
      (servers, shutdown) = result
      msg           =  stdout("Press Enter to stop.")
      _             <- (if(opts.openClient) openBrowser(port) *> msg else msg).liftM[MainErrT]
      _             <- Task.delay(waitForInput.runAsync(_ => shutdown.run)).liftM[MainErrT]
                    // We need to run the servers in order to make sure everything is cleaned up properly
      _             <- servers.run.liftM[MainErrT]
    } yield ()

    exec.swap
      .flatMap(e => stderr(e).liftM[EitherT[?[_], Unit, ?]])
      .merge
      .handleWith { case err => stderr(err.getMessage) }
      .run
  }
}
