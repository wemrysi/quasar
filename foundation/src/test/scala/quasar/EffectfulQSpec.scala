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

import slamdata.Predef.{Either, String, Throwable}

import scala.concurrent.ExecutionContext

import cats.effect.{Effect, IO}
import fs2.async.Promise
import org.specs2.execute.AsResult
import org.specs2.specification.core.Fragment

abstract class EffectfulQSpec[F[_]: Effect](implicit ec: ExecutionContext) extends Qspec {
  /** Provides syntax for defining effectful examples:
    *
    * "some example name" >>* {
    *   <example body returning F[A]>
    * }
    */
  implicit class RunExample(s: String) {
    def >>*[A: AsResult](fa: => F[A]): Fragment = {
      val run = for {
        p <- Promise.empty[IO, Either[Throwable, A]]
        _ <- Effect[F].runAsync(fa)(p.complete(_))
        r <- p.get
        a <- IO.fromEither(r)
      } yield a

      s >> run.unsafeRunSync
    }
  }
}
