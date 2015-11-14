package quasar
package api
package services

import Predef._
import fp._
import argonaut.Json

import org.http4s.Uri.Authority
import org.http4s.{Status, Method, Uri, Request}
import org.http4s.argonaut._
import org.specs2.mutable.Specification
import quasar.api.Server.Configuration

import scala.concurrent.duration.Duration
import scalaz.concurrent.Task
import scalaz.concurrent.Strategy.DefaultTimeoutScheduler

import scalaz._, Scalaz._

class ServerServiceSpec extends Specification {

  val client = org.http4s.client.blaze.defaultClient

  def withServerExpectingRestart[B](timeoutMillis: Long = 10000, initialPort: Int = 8888, defaultPort: Int = 8888)
                                      (causeRestart: Uri => Task[Unit])(afterRestart: Task[B]): B = {
    val uri = Uri(authority = Some(Authority(port = Some(initialPort))))

    val servers = Server.startServers(initialPort, reload => ListMap("" -> server.service(defaultPort,reload)))

    (for {
      result <- servers
      (servers, shutdown) = result
      b <- (for {
        unconsResult <- servers.unconsOption
        (_, others) = unconsResult.get
        _ <- causeRestart(uri)
        unconsResult2 <- others.unconsOption
        (_, others2) = unconsResult2.get
        b <- afterRestart
        _ <- shutdown
        _ <- others2.run
      } yield b).timed(timeoutMillis)(DefaultTimeoutScheduler).onFinish(_ => shutdown)
    } yield b).runFor(timeoutMillis)
  }

  "Server Service" should {
    "be capable of providing it's name and version" in {
      val request = Request(uri = Uri(path = "info"), method = Method.GET)
      val response = server.service(8888, _ => Task.now(()))(request).run
      response.as[Json].run must_== server.nameAndVersionInfo
      response.status must_== Status.Ok
    }
    def checkRunningOn(port: Int) = {
      val req = Request(uri = Uri(authority = Some(Authority(port = Some(port)))) / "info", method = Method.GET)
      client(req).map(response => response.status must_== Status.Ok)
    }
    "restart on new port when PUT /port succeeds" in {
      val newPort = 8889

      withServerExpectingRestart(){ baseUri: Uri =>
        for {
          req <- Request(uri = baseUri / "port", method = Method.PUT).withBody(newPort.toString)
          _   <- client(req)
        } yield ()
      }{ checkRunningOn(newPort) }
    }
    "restart on default port when DELETE /port succeeds" in {
      val defaultPort = 9001
      withServerExpectingRestart(initialPort = 9000, defaultPort = defaultPort){ baseUri: Uri =>
        val req = Request(uri = baseUri / "port", method = Method.DELETE)
        client(req).void
      }{ checkRunningOn(defaultPort) }
    }
  }
}
