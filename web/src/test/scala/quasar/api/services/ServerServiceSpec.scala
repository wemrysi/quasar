package quasar
package api
package services

import Predef._
import argonaut.Json

import org.http4s.Uri.Authority
import org.http4s.{Status, Method, Uri, Request}
import org.http4s.argonaut._
import org.specs2.mutable.Specification
import quasar.api.Server.Configuration

import scala.concurrent.duration.Duration
import scalaz.concurrent.Task
import scalaz.stream._

class ServerServiceSpec extends Specification {

  def withServerExpectingRestart[B](timeoutMillis: Long = 10000, port: Int = 8888)
                                      (causeRestart: Uri => Task[Unit])(afterRestart: Task[B]): B = {
    val uri = Uri(authority = Some(Authority(port = Some(port))))

    val configQ = async.boundedQueue[Configuration](1)

    val initialConfig = {
      def restart(port: Int): Task[Unit] =
        configQ.enqueueOne(
          Configuration(port, Duration.Inf, ListMap("" -> server.service(port, restart)))).map(_ => ())
      Configuration(port, Duration.Inf, ListMap("" -> server.service(port, restart)))
    }

    val servers = Server.servers(Process.emit(initialConfig) ++ configQ.dequeue)

    (for {
      _ <- servers.take(1).run
      _ <- causeRestart(uri)
      _ <- servers.drop(1).take(1).run
      b <- afterRestart
    } yield b).runFor(timeoutMillis)
  }

  "Server Service" should {
    "be capable of providing it's name and version" in {
      val request = Request(uri = Uri(path = "info"), method = Method.GET)
      val response = server.service(8888, _ => Task.now(()))(request).run
      response.as[Json].run must_== server.nameAndVersionInfo
      response.status must_== Status.Ok
    }
    "restart on new port when PUT /port succeeds" in {
      val newPort = 8889
      val client = org.http4s.client.blaze.defaultClient

      withServerExpectingRestart(){ baseUri: Uri =>
        for {
          req <- Request(uri = baseUri / "port", method = Method.PUT).withBody(newPort.toString)
          _   <- client(req)
        } yield ()
      }{
        val req = Request(uri = Uri(authority = Some(Authority(port = Some(newPort)))) / "info", method = Method.GET)
        client(req).map(response => response.status must_== Status.Ok)
      }
    }
  }
}
