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
import quasar.{Data, DataCodec}
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.fp.free._
import quasar.fs._
import quasar.fs.impl.ReadOpts
import quasar.physical.couchbase.common._

import scala.collection.JavaConverters._

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.json.JsonObject
import eu.timepit.refined.api.RefType.ops._
import monocle.macros.Lenses
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile {

  implicit val codec = DataCodec.Precise

  @Lenses final case class Cursor(bucket: Bucket, result: Vector[JsonObject])

  def interpret[S[_]](
    implicit
    S0: KeyValueStore[ReadFile.ReadHandle, Cursor, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Read[Context, ?] :<:  S,
    S3: Task :<: S
  ): ReadFile ~> Free[S, ?] =
    impl.read[S, Cursor](open, read, close)

  // TODO: stream results
  def open[S[_]](
    file: AFile, readOpts: ReadOpts
  )(implicit
    S1: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Cursor] =
    (for {
      ctx     <- context.ask.liftM[FileSystemErrT]
      bktCol  <- EitherT(bucketCollectionFromPath(file).point[Free[S, ?]])
      bkt     <- EitherT(getBucket(bktCol.bucket))
      limit   =  readOpts.limit.map(lim => s"LIMIT ${lim.unwrap}").orZero
      qStr    =  s"""SELECT d.* FROM `${bktCol.bucket}` d
                     WHERE type="${bktCol.collection}"
                     $limit OFFSET ${readOpts.offset.unwrap.shows}"""
      qResult <- lift(Task.delay(
                   bkt.query(n1qlQuery(qStr))
                     .rows
                     .asScala
                     .toVector
                     .map(_.value.getObject("value"))
                 )).into.liftM[FileSystemErrT]
    } yield Cursor(bkt, qResult)).run

  def read[S[_]](
    cursor: Cursor
  )(implicit
    S0: Task :<: S
  ): Free[S, FileSystemError \/ (Cursor, Vector[Data])] =
    lift(Task.delay(
      cursor.result.headOption.cata(
        jObj => DataCodec.parse(jObj.toString).bimap(
          err => FileSystemError.readFailed(jObj.toString, err.shows),
          d => (Cursor.result.modify(_.tail)(cursor), Vector(d))),
        (cursor, Vector.empty[Data]).right)
    )).into

  def close[S[_]](
    cursor: Cursor
  )(implicit
    S0: Task :<: S
  ): Free[S, Unit] =
    ().point[Free[S, ?]]

}
