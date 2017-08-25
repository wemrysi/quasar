/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar._
import quasar.common.{PhaseResult, PhaseResultT}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.kleisli._
import quasar.fs._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}
import quasar.physical.mongodb._

import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz._

final case class QueryContext[M[_]](
  model: MongoQueryModel,
  statistics: Collection => Option[CollectionStatistics],
  indexes: Collection => Option[Set[Index]],
  listContents: qscript.DiscoverPath.ListContents[M])

object QueryContext {

  import queryfileTypes._
  import FileSystemError._

  type MongoLogWF[C, A]  = PhaseResultT[QueryRT[MongoDbIO, C, ?], A]
  type MongoLogWFR[C, A] = FileSystemErrT[MongoLogWF[C, ?], A]

  val lpr = new LogicalPlanR[Fix[LogicalPlan]]

  def collections(lp: Fix[LogicalPlan]): PathError \/ Set[Collection] =
    // NB: documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
    lpr.paths(lp).toList
      .traverse(file => Collection.fromFile(mkAbsolute(rootDir, file)))
      .map(_.toSet)

  def queryContext[C](lp: Fix[LogicalPlan]):
      MongoLogWFR[C, QueryContext[FileSystemErrT[PhaseResultT[MongoDbIO, ?], ?]]] = {
    def lift[A](fa: FileSystemErrT[MongoDbIO, A]): MongoLogWFR[C, A] =
      EitherT[MongoLogWF[C, ?], FileSystemError, A](
        fa.run.liftM[QueryRT[?[_], C, ?]].liftM[PhaseResultT])

    def lookup[A](f: Collection => MongoDbIO[A]): EitherT[MongoDbIO, FileSystemError, Map[Collection, A]] =
      for {
        colls <- EitherT.fromDisjunction[MongoDbIO](collections(lp).leftMap(pathErr(_)))
        a     <- colls.toList.traverse(c => f(c).strengthL(c)).map(Map(_: _*)).liftM[FileSystemErrT]
      } yield a

    lift(
      (MongoDbIO.serverVersion.liftM[FileSystemErrT] |@|
        lookup(MongoDbIO.collectionStatistics) |@|
        lookup(MongoDbIO.indexes))((vers, stats, idxs) =>
        QueryContext(
          MongoQueryModel(vers),
          stats.get(_),
          idxs.get(_),
          dir => EitherT(WriterT(listContents(dir).run.map((Vector[PhaseResult](), _)))))))
  }
}
