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
  val blocker = Blocker.unsafeCached("sql2-compiler-spec")

  implicit val phaseResultW = MonadTell_.ignore[EitherT[Eval, QuasarError, ?], PhaseResults]

  val compile =
    Sql2Compiler[Fix, EitherT[Eval, QuasarError, ?]]
      .local[String](s => SqlQuery(Query(s), Variables.empty, Path.rootDir))

  "compile a query with very large FreeMap without overflowing stack" >> {
    val queryName = "/test-queries/large-freemap-soe.sql"
    val queryIS = IO(getClass.getResourceAsStream(queryName))

    val query =
      io.readInputStream(queryIS, 8192, blocker)
        .through(text.utf8Decode)
        .compile
        .foldMonoid
        .unsafeRunSync

    compile(query).value.value must not(throwA[Throwable])
  }
}
