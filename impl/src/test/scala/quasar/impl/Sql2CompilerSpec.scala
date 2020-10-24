/*
 * Copyright 2020 Precog Data
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

package quasar.impl

import quasar.Variables
import quasar.common.PhaseResults
import quasar.concurrent.unsafe._
import quasar.contrib.scalaz.MonadTell_
import quasar.sql.Query

import scala.StringContext
import scala.concurrent.duration._
import java.lang.{String, Throwable}

import cats.Eval
import cats.data.EitherT
import cats.implicits._
import cats.effect.{Blocker, IO}
import cats.effect.testing.specs2.CatsIO

import fs2.{io, text}

import matryoshka.data.Fix

import org.specs2.mutable.Specification

import pathy.Path

import shims.applicativeToScalaz

object Sql2CompilerSpec extends Specification with CatsIO {

  addSections

  override val Timeout = 30.seconds

  val blocker = Blocker.unsafeCached("sql2-compiler-spec")

  implicit val phaseResultW = MonadTell_.ignore[EitherT[Eval, QuasarError, ?], PhaseResults]

  val compile =
    Sql2Compiler[Fix, EitherT[Eval, QuasarError, ?]]
      .local[String](s => SqlQuery(Query(s), Variables.empty, Path.rootDir))

  def queryResource(fileName: String): IO[String] = {
    val queryIS = IO(getClass.getResourceAsStream(s"/test-queries/$fileName"))

    io.readInputStream(queryIS, 8192, blocker)
      .through(text.utf8Decode)
      .compile
      .foldMonoid
  }

  def compiles(queryName: String) =
    queryResource("case-variations.sql")
      .map(compile(_))
      .map(_.value.value must beRight)

  "stack safety" >> {
    "query with very large FreeMap" >> {
      val query = queryResource("large-freemap-soe.sql")
      compile(query.unsafeRunSync).value.value must not(throwA[Throwable])
    }
  }

  "hashCode abnormalities" >> {
    // Compile times for these explode if we don't emit strict `FreeMap`s from `CoalesceUnaryMappable`
    "case-variations" >> compiles("case-variations.sql")
    "customer-orders" >> compiles("customer-orders.sql")
    "demo-games-step1" >> compiles("demo-games-step1.sql")
    "demo-games-step2" >> compiles("demo-games-step2.sql")
  }
}
