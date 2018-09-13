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

package quasar.run

import slamdata.Predef._
import quasar.api.resource._
import quasar.contrib.cats.effect._
import quasar.contrib.pathy.AFile
import quasar.contrib.std.uuid._
import quasar.impl.datasources.DatasourceManagement.Running
import quasar.impl.evaluate.Source
import quasar.mimir.evaluate.QueryAssociate
import quasar.precog.common.RValue
import quasar.run.optics.{stringUuidP => UuidString}

import java.util.UUID

import cats.arrow.FunctionK
import cats.effect.{Effect, IO}
import cats.syntax.applicative._
import cats.syntax.functor._
import fs2.Stream
import scalaz.~>
import scalaz.std.option._
import scalaz.syntax.foldable._

/** Translates resource paths from queries into actual sources. */
object ResourceRouter {
  val DatasourceResourcePrefix = "datasource"

  def apply[T[_[_]], F[_]: Effect](
      datasources: F[Running[UUID, T, F]])(
      file: AFile)
      : F[Option[Source[QueryAssociate[T, IO]]]] = {

    val resultsToIO =
      new (λ[a => F[Stream[F, a]]] ~> λ[a => IO[Stream[IO, a]]]) {
        def apply[A](fa: F[Stream[F, A]]): IO[Stream[IO, A]] =
          fa.to[IO].map(_.translate(λ[FunctionK[F, IO]](_.to[IO])))
      }

    ResourcePath.leaf(file) match {
      case DatasourceResourcePrefix /: UuidString(id) /: qaPath =>
        datasources.map(_.lookup(id) map { d =>
          val qa = d.unsafeValue.fold[QueryAssociate[T, IO]](
            lw => QueryAssociate.lightweight(f =>
              resultsToIO(lw.evaluator[RValue].evaluate(f))),
            hw => QueryAssociate.heavyweight(q =>
              resultsToIO(hw.evaluator[RValue].evaluate(q))))

          Source(qaPath, qa)
        })

      case _ => none[Source[QueryAssociate[T, IO]]].pure[F]
    }
  }
}
