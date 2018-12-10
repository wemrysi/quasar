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

package quasar.mimir.evaluate

import quasar.api.QueryEvaluator
import quasar.api.datasource.DatasourceRef
import quasar.api.table.TableRef
import quasar.common.PhaseResultTell
import quasar.contrib.scalaz.MonadError_
import quasar.contrib.pathy.ADir
import quasar.impl.DatasourceModule
import quasar.impl.datasources.DatasourceManagement.Running
import quasar.impl.evaluate.FederatingQueryEvaluator
import quasar.impl.schema.SstEvalConfig
import quasar.impl.storage.IndexedStore
import quasar.impl.table.PreparationsManager
import quasar.mimir.{MimirRepr, Precog}
import quasar.mimir.evaluate.optics._
import quasar.mimir.storage.{MimirIndexedStore, MimirPTableStore, PTableSchema, StoreKey}
import quasar.run.{MonadQuasarErr, Quasar, Sql2QueryEvaluator, SqlQuery}
import quasar.run.implicits._
import quasar.yggdrasil.vfs.ResourceError

import java.util.UUID
import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.{~>, Monad}
import cats.effect.concurrent.Ref
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.applicative._
import cats.syntax.functor._
import fs2.Stream
import matryoshka.data.Fix
import pathy.Path._
import shims._

final class QuasarImpl[F[_]: ConcurrentEffect: Monad: MonadQuasarErr: PhaseResultTell: MonadError_[?[_], ResourceError]](
    precog: Precog)(
    implicit
    cs: ContextShift[IO],
    ec: ExecutionContext) {

  import QuasarImpl._

  val datasourceRefs: IndexedStore[F, UUID, DatasourceRef[Json]] =
    MimirIndexedStore.transformValue(
      MimirIndexedStore.transformIndex(
        MimirIndexedStore[F](precog, DatasourceRefsLocation),
        "UUID",
        StoreKey.stringIso composePrism stringUuidP),
      "DatasourceRef",
      rValueDatasourceRefP(rValueJsonP))

  val tableRefs: IndexedStore[F, UUID, TableRef[SqlQuery]] =
    MimirIndexedStore.transformValue(
      MimirIndexedStore.transformIndex(
        MimirIndexedStore[F](precog, TableRefsLocation),
        "UUID",
        StoreKey.stringIso composePrism stringUuidP),
      "TableRef",
      rValueTableRefP(rValueSqlQueryP))

  val pTableStore = MimirPTableStore[F](precog, PreparationLocation)

  val pushdown = new PushdownControl(Ref.unsafe[F, Pushdown](Pushdown.EnablePushdown))

  val federation = MimirQueryFederation[Fix, F](precog, pushdown)
  
  val getQueryEvaluator: F[Running[UUID, Fix, F]] => QueryEvaluator[F, SqlQuery, Stream[F, MimirRepr]] =
    running =>
      Sql2QueryEvaluator(FederatingQueryEvaluator(federation, ResourceRouter(running)))
        .map(_.translate(λ[IO ~> F](_.to[F])))

  val preparationsManager: F[Running[UUID, Fix, F]] => Stream[F, PreparationsManager[F, UUID, SqlQuery, Stream[F, MimirRepr]]] =
    running =>
      PreparationsManager[F, UUID, SqlQuery, Stream[F, MimirRepr]](getQueryEvaluator(running)) {
        case (key, table) =>
          ConcurrentEffect[F].delay {
            table.flatMap(data =>
              pTableStore.write(
                storeKeyUuidP.reverseGet(key),
                data.table.asInstanceOf[pTableStore.cake.Table])) // yolo
          }
      }

  val lookupFromPTableStore: UUID => F[Option[Stream[F, MimirRepr]]] =
    key => pTableStore.read(storeKeyUuidP.reverseGet(key))
      .map(_.map(t =>
        Stream(MimirRepr(pTableStore.cake)(
          t.asInstanceOf[pTableStore.cake.Table])).covary[F])) // yolo x2

  val lookupTableSchema: UUID => F[Option[PTableSchema]] =
    key => pTableStore.schema(storeKeyUuidP.reverseGet(key))
}

object QuasarImpl {

  // The location of the datasource refs tables within `mimir`.
  val DatasourceRefsLocation: ADir =
    rootDir </> dir("quasar") </> dir("datasource-refs")

  val TableRefsLocation: ADir =
    rootDir </> dir("quasar") </> dir("table-refs")

  val PreparationLocation: ADir =
    rootDir </> dir("quasar") </> dir("preparations")

  def apply[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: MonadError_[?[_], ResourceError]: PhaseResultTell: Timer](
      precog: Precog,
      datasourceModules: List[DatasourceModule],
      sstEvalConfig: SstEvalConfig)(
      implicit
      cs: ContextShift[IO],
      ec: ExecutionContext)
      : Stream[F, (PushdownControl[F], Quasar[F, MimirRepr, PTableSchema])] = {

    val impl = new QuasarImpl[F](precog)

    val qs = Quasar[F, MimirRepr, PTableSchema](
      impl.datasourceRefs,
      impl.tableRefs,
      impl.getQueryEvaluator,
      impl.preparationsManager,
      impl.lookupFromPTableStore,
      impl.lookupTableSchema,
      datasourceModules,
      sstEvalConfig)

    for {
      p <- Stream.eval(impl.pushdown.pure[F])
      q <- qs
    } yield (p, q)
  }
}
