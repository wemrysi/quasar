package quasar
package api
package services

import Predef._

import org.http4s.{Uri, Request}
import org.specs2.mutable.Specification

class WelcomeServiceSpec extends Specification {
  "Welcome service" should {
    "show a welcome message" in {
      val req = Request()
      val resp = welcome.service(req).run
      resp.as[String].run must contain("quasar-logo-vector.png")
    }
    "show the current version" in {
      val req = Request()
      val resp = welcome.service(req).run
      resp.as[String].run must contain("Quasar " + build.BuildInfo.version)
    }
  }
}
