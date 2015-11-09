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
import quasar.fp._
import quasar.console._
import quasar._, Errors._, Evaluator._
import quasar.config._

import java.io.File
import java.lang.System
import quasar.fs.inmemory._

import scala.concurrent.duration._

import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import org.http4s.server.{Server => Http4sServer, HttpService}
import org.http4s.server.blaze.BlazeBuilder

import quasar.api.services.RestApi

object Server {
  // NB: This is a terrible thing.
  //     Is there a better way to find the path to a jar?
  val jarPath: Task[String] =
    Task.delay {
      val uri = Server.getClass.getProtectionDomain.getCodeSource.getLocation.toURI
      val path0 = uri.getPath
      val path =
        java.net.URLDecoder.decode(
          if (path0 == null)
            uri.toURL.openConnection.asInstanceOf[java.net.JarURLConnection].getJarFileURL.getPath
          else path0,
          "UTF-8")
      (new File(path)).getParentFile().getPath() + "/"
    }

  /** Returns why the given port is unavailable or None if it is available. */
  def unavailableReason(port: Int): OptionT[Task, String] =
    OptionT(Task.delay(new java.net.ServerSocket(port)).attempt.flatMap {
      case -\/(err: java.net.BindException) => Task.now(Some(err.getMessage))
      case -\/(err)                         => Task.fail(err)
      case \/-(s)                           => Task.delay(s.close()).as(None)
    })

  /** An available port number. */
  def anyAvailablePort: Task[Int] = Task.delay {
    val s = new java.net.ServerSocket(0)
    val p = s.getLocalPort
    s.close()
    p
  }

  /** Returns the requested port if available, or the next available port. */
  def choosePort(requested: Int): Task[Int] =
    unavailableReason(requested)
      .flatMapF(rsn => stderr("Requested port not available: " + requested + "; " + rsn) *>
                       anyAvailablePort)
      .getOrElse(requested)

  case class Configuration(port: Int, idleTimeout: Duration, svcs: ListMap[String, HttpService])

  /** Start `Server` with supplied [[Configuration]]
    * @param flexibleOnPort Whether or not to choose an alternative port if requested port is not available
    * @return Server that has been started along with the port on which it was started
    */
  def startServer(config : Configuration, flexibleOnPort: Boolean): Task[(Http4sServer, Int)] = {
    for {
      actualPort <- if (flexibleOnPort) choosePort(config.port) else Task.now(config.port)
      builder <- Task.delay {
        val initialBuilder = BlazeBuilder
          .withIdleTimeout(config.idleTimeout)
          .bindHttp(config.port, "0.0.0.0")

        config.svcs.toList.reverse.foldLeft(initialBuilder) {
          case (b, (path, svc)) => b.mountService(Prefix(path)(svc))
        }
      }
      server <- Task.delay(builder.run)
    } yield (server, actualPort)
  }

  final case class StaticContent(loc: String, path: String)

  /** Given a [[Process]] of [[quasar.api.Server.Configuration]], returns a [[Process]] of [[org.http4s.server.Server]].
    *
    * The returned process will emit each time a new server configuration is provided and ensures only
    * one server is running at a time, i.e. providing a new Configuration ensures
    * the previous server has been stopped.
    *
    * When the process of configurations terminates for any reason, the last server is shutdown and the
    * process of servers will terminate.
    * @param flexibleOnPort Whether or not to choose an alternative port if requested port is not available
    */
  def servers(configurations: Process[Task, Configuration], flexibleOnPort: Boolean): Process[Task, (Http4sServer,Int)] = {

    val serversAndPort = configurations.evalMap(conf =>
      startServer(conf, flexibleOnPort).onSuccess{ case (_,port) =>
        stdout("Server started. Listening on port " + port)})

    serversAndPort.evalScan1{case ((oldServer, oldPort), newServerAndPort) =>
      oldServer.shutdown.flatMap(_ => stdout("Stopped server listening on port " + oldPort)) *>
      Task.now(newServerAndPort)
    }.cleanUpWithA{ server =>
      server.map { case (lastServer, lastPort) =>
        lastServer.shutdown.flatMap(_ => stdout("Stopped last server listening on port " + lastPort))
      }.getOrElse(Task.now(()))
    }
  }

  // Lifted from unfiltered.
  // NB: available() returns 0 when the stream is closed, meaning the server
  //     will run indefinitely when started from a script.
  private def waitForInput: Task[Unit] = for {
    _    <- Task.delay(java.lang.Thread.sleep(250))
                .handle { case _: java.lang.InterruptedException => () }
    test <- Task.delay(System.console == null || System.in.available() <= 0)
                .handle { case _ => true }
    done <- if (test) waitForInput else Task.now(())
  } yield done

  private def openBrowser(port: Int): Task[Unit] = {
    val url = "http://localhost:" + port + "/"
    Task.delay(java.awt.Desktop.getDesktop().browse(java.net.URI.create(url)))
        .or(stderr("Failed to open browser, please navigate to " + url))
  }

  case class Options(
    config: Option[String],
    contentLoc: Option[String],
    contentPath: Option[String],
    contentPathRelative: Boolean,
    openClient: Boolean,
    port: Option[Int])

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

  def interpretPaths(options: Options): EnvTask[Option[StaticContent]] = {
    val defaultLoc = "/files"

    def path(p: String): EnvTask[String] =
      liftE(
        if (options.contentPathRelative) jarPath.map(_ + p)
        else Task.now(p))

    (options.contentLoc, options.contentPath) match {
      case (None, None) => none.point[EnvTask]

      case (Some(_), None) => EitherT.left(Task.now(InvalidConfig("content-location specified but not content-path")))

      case (loc, Some(p)) => path(p).map(p => Some(StaticContent(loc.getOrElse(defaultLoc), p)))
    }
  }

  def main(args: Array[String]): Unit = {
    val idleTimeout = Duration.Inf

    val exec: EnvTask[Unit] = for {
      opts           <- optionParser.parse(args, Options(None, None, None, false, false, None)).cata(
                          _.point[EnvTask],
                          EitherT.left(Task.now(InvalidConfig("couldn’t parse options"))))
      content <- interpretPaths(opts)
      interpreter = runStatefully(InMemState.empty).run.compose(filesystem)
      redirect = content.map(_.loc)
      port           =  opts.port getOrElse 8080
      configQ        = async.boundedQueue[Configuration](1)
      config         = {
        def reload(port: Int): Task[Unit] = configQ.enqueueOne(Configuration(port,idleTimeout, RestApi(content.toList,redirect, port, reload).AllServices(interpreter)))
        Configuration(port,idleTimeout, RestApi(content.toList,redirect, port,reload).AllServices(interpreter))
      }
      // Is isFlexibleOnPort = true what we want here, it seems this could be non ideal, because to my knowlegde
      // we do not send the port back to the client, so how is he to figure out that it succe
      _ <-  servers(configQ.dequeue, true).run.liftM[EnvErrT]
      msg = stdout("Press Enter to stop.")
      _ <- (if(opts.openClient) openBrowser(port) *> msg else msg).liftM[EnvErrT]
      _ <- Task.fork(waitForInput.onFinish(_ => configQ.close)).liftM[EnvErrT]
    } yield ()

    exec.swap
      .flatMap(e => stderr(e.message).liftM[EitherT[?[_], Unit, ?]])
      .merge
      .handleWith { case err => stderr(err.getMessage) }
      .run
  }
}
