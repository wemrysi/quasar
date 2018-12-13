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

package quasar.impl.datasources

import slamdata.Predef.Option

import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, QueryResult}
import quasar.ejson.EJson
import quasar.impl.parsing.ResourceParser
import quasar.impl.schema._
import quasar.sst.SST

import scala.concurrent.duration.FiniteDuration

import cats.effect.{Concurrent, Timer}
import cats.syntax.functor._

import matryoshka.{Corecursive, Recursive}

import qdata.QDataEncode

import scalaz.Order

import spire.algebra.Field
import spire.math.ConvertableTo

final class QueryResultSst[
    F[_]: Concurrent: MonadResourceErr,
    J: Order,
    N: ConvertableTo: Field: Order] private (
    evalConfig: SstEvalConfig)(
    implicit
    timer: Timer[F],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson])
    extends ResourceSchema[F, SstConfig[J, N], (ResourcePath, QueryResult[F])] {

  def apply(
      sstConfig: SstConfig[J, N],
      resource: (ResourcePath, QueryResult[F]),
      timeLimit: FiniteDuration)
      : F[Option[sstConfig.Schema]] = {

    type S = SST[J, N]

    implicit val sstQDataEncode: QDataEncode[S] =
      QDataCompressedSst.encode[J, N](sstConfig)

    val (path, content) = resource

    val k: N = ConvertableTo[N].fromLong(evalConfig.sampleSize.value)

    ResourceParser[F, S](path, content)
      .take(evalConfig.sampleSize.value)
      .chunkLimit(evalConfig.chunkSize.value.toInt)
      .through(ProgressiveSst.async(sstConfig, evalConfig.parallelism.value.toInt))
      .interruptWhen(Concurrent[F].attempt(timer.sleep(timeLimit)))
      .compile.last
      .map(_.map(SstSchema.fromSampled(k, _)))
  }
}

object QueryResultSst {
  def apply[
    F[_]: Concurrent: MonadResourceErr,
    J: Order,
    N: ConvertableTo: Field: Order](
    evalConfig: SstEvalConfig)(
    implicit
    timer: Timer[F],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson])
    : ResourceSchema[F, SstConfig[J, N], (ResourcePath, QueryResult[F])] =
  new QueryResultSst[F, J, N](evalConfig)
}
