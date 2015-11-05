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

import argonaut.CodecJson
import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import org.http4s.server.{Server => Http4sServer, HttpService}
import org.http4s.server.blaze.BlazeBuilder

object ServerOps {
  final case class Options(
    config: Option[String],
    contentLoc: Option[String],
    contentPath: Option[String],
    contentPathRelative: Boolean,
    openClient: Boolean,
    port: Option[Int])
}

class ServerOps[WC: CodecJson, SC](
  configOps: ConfigOps[WC],
  defaultWC: WC,
  val webConfigLens: WebConfigLens[WC, SC]) {
  import webConfigLens._
  import ServerOps._

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

  case class ServerBlueprint(config: WC, idleTimeout: Duration, svcs: ListMap[String, HttpService])

  /** Start `Server` with supplied [[ServerBlueprint]] */
  def startServer(blueprint : ServerBlueprint): Task[Http4sServer] = {
    val initialBuilder = BlazeBuilder
      .withIdleTimeout(blueprint.idleTimeout)
      .bindHttp(wcPort.get(blueprint.config), "0.0.0.0")

    val builder = blueprint.svcs.toList.reverse.foldLeft(initialBuilder) {
      case (b, (path, svc)) => b.mountService(Prefix(path)(svc))
    }
    Task.delay(builder.run)
  }

  final case class StaticContent(loc: String, path: String)

  implicit class AugmentedProcess[A](p: Process[Task,A]) {
    def evalScan1(f: (A, A) => Task[A]): Process[Task, A] = {
      p.zipWithPrevious.evalMap {
        case (None, next) => Task.now(next)
        case (Some(prev), next) => f(prev, next)
      }
    }
  }

  /**
   * Given a process of [[ServerBlueprint]], returns a process of [[org.http4s.server.Server]].
   *
   * The process will emit each time a new server configuration is provided and ensures only
   * one server is running at a time, i.e. providing a new builder ensures
   * the previous server has been stopped.
   *
   * Simply terminate the process of builders in order to shutdown any running server and prevent
   * new ones from being started.
   */
  def servers(triggerRestartWithConfiguration: Process[Task, Configuration]): Process[Task, (Int,Http4sServer)] = {

    val serversAndPort = triggerRestartWithConfiguration.evalMap(conf =>
      startServer(conf).map((conf.port, _)) <* stdout("Server started listening on port " + conf.port))

    serversAndPort.evalScan1{case ((oldPort, oldServer), newServerAndPort) => oldServer.shutdown *> stdout("Stopped server listening on port " + oldPort) *> Task.now(newServerAndPort)}
  }

  // Lifted from unfiltered.
  // NB: available() returns 0 when the stream is closed, meaning the server
  //     will run indefinitely when started from a script.
  private def waitForInput: Task[Unit] = for {
    _    <- Task.delay(java.lang.Thread.sleep(250))
                .handle { case _: java.lang.InterruptedException => () }
    test <- Task.delay(Option(System.console).isEmpty || System.in.available() <= 0)
                .handle { case _ => true }
    done <- if (test) waitForInput else Task.now(())
  } yield done

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

    def startAndStopOnkeyEntered(conf: Configuration, openClient: Boolean): Process[Task, Configuration] = {
      val msg = stdout("Press Enter to stop.")
      val start = if (openClient) openBrowser(conf.port) *> msg else msg
      val alive = async.signalUnset[Nothing]
      val waitAndClose = waitForInput.onFinish(_ => alive.close)
      Process.eval_(start *> Task.fork(waitAndClose)) ++ alive.discrete
    }

    val exec: EnvTask[Unit] = for {
      opts           <- optionParser.parse(args, Options(None, None, None, false, false, None)).cata(
                          _.point[EnvTask],
                          EitherT.left(Task.now(InvalidConfig("couldn’t parse options"))))
      content <- interpretPaths(opts)
      interpreter = runStatefully(InMemState.empty).run.compose(filesystem)
      redirect = content.map(_.loc)
      port           =  opts.port getOrElse 8080
      config         = Configuration(port,idleTimeout, new RestApi(content.toList,redirect, port,???).AllServices(interpreter))
      _ <-  servers(startAndStopOnkeyEntered(config, opts.openClient)).run.liftM[EnvErrT]
    } yield ()

    exec.swap
      .flatMap(e => stderr(e.message).liftM[EitherT[?[_], Unit, ?]])
      .merge
      .handleWith { case err => stderr(err.getMessage) }
      .run
  }
}

// https://github.com/puffnfresh/wartremover/issues/149
@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
object Server extends ServerOps(
  WebConfig,
  WebConfig(ServerConfig(None), Map()),
  WebConfigLens(
    WebConfig.server,
    WebConfig.mountings,
    WebConfig.server composeLens ServerConfig.port,
    ServerConfig.port))
