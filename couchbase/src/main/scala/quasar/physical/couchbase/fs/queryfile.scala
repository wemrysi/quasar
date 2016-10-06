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

package quasar.physical.couchbase.fs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.effect.{Read, MonotonicSeq}
import quasar.fp._, free._
import quasar.fp.ski._
import quasar.fs._
import quasar.physical.couchbase.common._
import quasar.qscript._

import scala.collection.JavaConverters._

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfile {
  import QueryFile._

  def interpret[S[_]](
    implicit
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S3: Task :<: S
  ): QueryFile ~> Free[S, ?] = λ[QueryFile ~> Free[S, ?]] {
    case ExecutePlan(lp, out) => ???
    case EvaluatePlan(lp)     => ???
    case More(h)              => ???
    case Close(h)             => ???
    case Explain(lp)          => ???
    case ListContents(dir)    => listContents(dir)
    case FileExists(file)     => fileExists(file)
  }

  def listRootContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      ctx      <- context.ask.liftM[FileSystemErrT]
      bktNames <- lift(Task.delay(
                    ctx.manager.getBuckets.asScala.toList.map(_.name)
                  )).into.liftM[FileSystemErrT]
      bkts     <- bktNames.traverse(n => EitherT(getBucket(n)))
      bktCols  <- bkts.traverseM(bkt => lift(Task.delay(
                    bkt.query(n1qlQuery(s"select distinct type from `${bkt.name}`"))
                      .allRows.asScala.toList.map(r =>
                        BucketCollection(bkt.name, new String(r.byteValue)))
                  )).into.liftM[FileSystemErrT])
    } yield pathSegmentsFromBucketCollections(bktCols)).run

  def listNonRootContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      ctx    <- context.ask.liftM[FileSystemErrT]
      bktCol <- EitherT(bucketCollectionFromPath(dir).point[Free[S, ?]])
      bkt    <- EitherT(getBucket(bktCol.bucket))
      docIds <- lift(docIdTypesWithTypePrefix(bkt, bktCol.collection)).into.liftM[FileSystemErrT]
      _      <- EitherT((
                  if (docIds.isEmpty) FileSystemError.pathErr(PathError.pathNotFound(dir)).left
                  else ().right
                ).point[Free[S, ?]])
    } yield pathSegmentsFromPrefixDocIds(bktCol.collection, docIds)).run

  def listContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    if (dir === rootDir)
      listRootContents(dir)
    else
      listNonRootContents(dir)

  def fileExists[S[_]](
    file: AFile
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, Boolean] =
    (for {
      ctx    <- context.ask.liftM[FileSystemErrT]
      bktCol <- EitherT(bucketCollectionFromPath(file).point[Free[S, ?]])
      bkt    <- EitherT(getBucket(bktCol.bucket))
      exists <- lift(existsWithPrefix(bkt, bktCol.collection)).into.liftM[FileSystemErrT]
    } yield exists).exists(ι)

}
