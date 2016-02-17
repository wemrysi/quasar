/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.server

import quasar.Predef._
import quasar.fp._

import org.http4s.Uri.Authority
import org.http4s.{Status, Method, Uri, Request}
import org.specs2.mutable

import argonaut.Json
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.concurrent.Strategy.DefaultTimeoutScheduler

class ControlServiceSpec extends mutable.Specification {

  val client = org.http4s.client.blaze.defaultClient

  def withServerExpectingRestart[B](timeoutMillis: Long = 30000, initialPort: Int = 8888, defaultPort: Int = 8888)
                                      (causeRestart: Uri => Task[Unit])(afterRestart: Task[B]): B = {
    val uri = Uri(authority = Some(Authority(port = Some(initialPort))))

    val servers = Http4sUtils.startServers(initialPort, reload => control.service(defaultPort, reload))

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

  "Control Service" should {
    def checkRunningOn(port: Int) = {
      val req = Request(uri = Uri(authority = Some(Authority(port = Some(port)))) / "foobar", method = Method.GET)
      client(req).map(response => response.status must_== Status.NotFound)
    }
    "restart on new port when PUT succeeds" in {
      val newPort = 8889

      withServerExpectingRestart(){ baseUri: Uri =>
        for {
          req <- Request(uri = baseUri, method = Method.PUT).withBody(newPort.toString)
          _   <- client(req)
        } yield ()
      }{ checkRunningOn(newPort) }
    }
    "restart on default port when DELETE succeeds" in {
      val defaultPort = 9001
      withServerExpectingRestart(initialPort = 9000, defaultPort = defaultPort){ baseUri: Uri =>
        val req = Request(uri = baseUri, method = Method.DELETE)
        client(req).void
      }{ checkRunningOn(defaultPort) }
    }
  }
}
