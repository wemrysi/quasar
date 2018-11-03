/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar

import slamdata.Predef.{Char, RuntimeException, Unit}

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.{IO, Resource}
import cats.effect.concurrent.Ref
import cats.syntax.all._
import shims._

object DisposableSpec extends EffectfulQSpec[IO] {
  "from resource" >> {
    "disposes all resources" >>* {
      for {
        dA <- Ref[IO].of(false)
        dB <- Ref[IO].of(false)

        rA = Resource(IO(('A', dA.set(true))))
        rB = Resource(IO(('B', dB.set(true))))
        rAB = rA.product(rB)

        disp <- Disposable.fromResource(rAB)

        _ <- disp.dispose

        adisposed <- dA.get
        bdisposed <- dB.get

      } yield {
        adisposed must beTrue
        bdisposed must beTrue
      }
    }

    "disposes initial resources when subsequent fail during acquisition" >>* {
      for {
        dA <- Ref[IO].of(false)

        rA = Resource(IO(('A', dA.set(true))))
        rB = Resource(IO.raiseError[(Char, IO[Unit])](new RuntimeException("YIKES!")))
        rAB = rA.product(rB)

        disp <- Disposable.fromResource(rAB).attempt

        adisposed <- dA.get
      } yield {
        disp must beLeft
        adisposed must beTrue
      }
    }
  }
}
