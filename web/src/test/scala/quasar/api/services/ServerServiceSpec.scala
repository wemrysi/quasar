package quasar
package api
package services

import org.http4s.Uri.Authority
import org.http4s.{Status, Method, Uri, Request}
import org.http4s.server.HttpService
import org.specs2.mutable.Specification
import quasar.api.Server.Configuration

import scala.concurrent.duration.Duration
import scalaz.{~>, Functor}
import scalaz.concurrent.Task

class ServerServiceSpec extends Specification {

  def withServerExpectingRestart[B, S[_]: Functor](f: S ~> Task)(timeoutMillis: Long = 10000, port: Int = 8888)
                                      (causeRestart: Task[Unit])(afterRestart: Task[B]): B = {
    type S = (Int, org.http4s.server.Server)

    val uri = Uri(authority = Authority(port = port))

    val config = Configuration(port, Duration.Inf, new RestApi(Nil, None).AllServices(f))

    for {
      server <- Server.startServer(config)
    }
  }

  "Server Service" should {
    def service: HttpService = ???
    "be capable of providing it's name and version" in {
      val request = Request(uri = Uri(path = "info"), method = Method.GET)
      val response = service(request).run
      response.as[String].run must_== versionAndNameInfo.toString
      response.status must_== Status.OK
    }
    "restart on new port when PUT /port succeeds" in {
      val newPort = 8889

      withServerExpectingRestart{ baseUri =>
        val client = org.http4s.client.blaze.defaultClient
        Request(uri = baseUri / "port", method = )
      }
    }
  }
}
