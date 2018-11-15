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

package quasar.mimir

import slamdata.Predef.{List, StringContext}

import quasar.{EffectfulQSpec, Variables}
import quasar.api.datasource._
import quasar.common.PhaseResults
import quasar.common.data.RValue
import quasar.concurrent.BlockingContext
import quasar.contrib.scalaz.{MonadError_, MonadTell_}
import quasar.impl.DatasourceModule
import quasar.impl.schema.SstEvalConfig
import quasar.regression.Sql2QueryRegressionSpec.TestsRoot
import quasar.run.{Quasar, QuasarError, SqlQuery}
import quasar.run.implicits._
import quasar.sql.Query

import scala.concurrent.ExecutionContext.Implicits.global

import java.nio.file.Paths

import argonaut.{Argonaut, Json}, Argonaut._
import cats.effect.{ContextShift, IO, Timer}
import eu.timepit.refined.auto._
import fs2.{Chunk, Stream}
import pathy.Path
import shims._

object MimirCompressedEvaluationSpec extends EffectfulQSpec[IO] with PrecogCake {
  lazy val blockingPool = BlockingContext.cached("mimir-compressed-evaluation-spec")

  override implicit lazy val cs: ContextShift[IO] =
    IO.contextShift(global)

  implicit def ignorePhaseResults: MonadTell_[IO, PhaseResults] =
    MonadTell_.ignore[IO, PhaseResults]

  implicit def ioQuasarError: MonadError_[IO, QuasarError] =
    MonadError_.facet[IO](QuasarError.throwableP)

  implicit def ioTimer: Timer[IO] =
    IO.timer(global)

  implicit def streamQuasarError: MonadError_[Stream[IO, ?], QuasarError] =
    MonadError_.facet[Stream[IO, ?]](QuasarError.throwableP)

  "successfully evaluates query involving gzipped resources" >>* {
    val results = for {
      q <- Quasar[IO](
        cake,
        List(DatasourceModule.Lightweight(TestGzippedLocalDatasourceModule)),
        SstEvalConfig.single)

      currentDir <- Stream.eval(IO(Paths.get("").toAbsolutePath))

      testsDir = TestsRoot(currentDir)

      dsRef = DatasourceRef(
        TestGzippedLocalDatasourceModule.kind,
        DatasourceName("test"),
        Json(
          "rootDir" := testsDir.toString,
          "readChunkSizeBytes" := 32768))

      r <- Stream.eval(q.datasources.addDatasource(dsRef))

      id <- r.fold(
        e => MonadError_[Stream[IO, ?], DatasourceError.CreateError[Json]].raiseError(e),
        i => Stream.emit(i).covary[IO])

      sql2 = s"""select count(*) from `/datasource/$id/zips.data` where state = "CA""""

      query = SqlQuery(Query(sql2), Variables.empty, Path.rootDir)

      repr <- Stream.force(q.queryEvaluator.evaluate(query))

      rv <- Stream.evalUnChunk(repr.table.toJson.map(rvs => Chunk.seq(rvs.toSeq)))
    } yield rv

    results.compile.last.map(_ must beSome(RValue.rLong(1516)))
  }
}
