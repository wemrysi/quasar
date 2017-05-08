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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadReader_
import quasar.fs.FileSystemError

import scala.collection.JavaConverters._

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.query.{N1qlParams, N1qlQuery, N1qlQueryRow}
import com.couchbase.client.java.query.consistency.ScanConsistency
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object common {

  type BucketNameReader[F[_]] = MonadReader_[F, BucketName]

  object BucketNameReader {
    def apply[F[_]](implicit R: MonadReader_[F, BucketName]) = R
  }

  final case class Context(bucket: Bucket)

  final case class BucketName(v: String)

  // type field in Couchbase documents
  final case class DocType(v: String)

  final case class Cursor(result: Vector[Data])

  val CBDataCodec = DataCodec.Precise

  def docTypeFromPath(p: APath): DocType =
    DocType(Path.flatten(None, None, None, Some(_), Some(_), p).toIList.intercalate("/".some).orZero)

  def deleteHavingPrefix(
    bucket: Bucket,
    prefix: String
  ): Task[FileSystemError \/ Unit] = {
    val qStr = s"""DELETE FROM `${bucket.name}` WHERE type LIKE "${prefix}%""""

    query(bucket, qStr).map(_.void)
  }

  def docTypesFromPrefix(
    bucket: Bucket,
    prefix: String
  ): Task[FileSystemError \/ List[DocType]] = {
    val qStr = s"""SELECT distinct type FROM `${bucket.name}`
                   WHERE type LIKE "${prefix}%""""
    query(bucket, qStr).map(_.map(_.toList.map(r => DocType(r.value.getString("type")))))
  }

  def existsWithPrefix(
    bucket: Bucket,
    prefix: String
  ): Task[FileSystemError \/ Boolean] = {
    val qStr = s"""SELECT count(*) > 0 v FROM `${bucket.name}`
                   WHERE type = "${prefix}" OR type like "${prefix}/%""""
    query(bucket, qStr).map(_.map(_.toList.exists(_.value.getBoolean("v").booleanValue === true)))
  }

  def pathSegments(paths: List[List[String]]): Set[PathSegment] =
    paths.collect {
      case h :: Nil => FileName(h).right
      case h :: _   => DirName(h).left
    }.toSet

  //def pathSegmentsFromBucketCollections(bktCols: List[String]): Set[PathSegment] =
  //  pathSegments(bktCols.map(bc => bc.bucket :: bc.collection.split("/").toList))

  def pathSegmentsFromPrefixTypes(prefix: String, types: List[DocType]): Set[PathSegment] =
    pathSegments(types.map(_.v.stripPrefix(prefix).stripPrefix("/").split("/").toList))

  def query(bucket: Bucket, query: String): Task[FileSystemError \/ Vector[N1qlQueryRow]] =
    Task.delay {
      val qr = bucket.query(N1qlQuery.simple(
        query, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)))
      qr.errors.asScala.toVector.toNel.cata(
        errors => FileSystemError.readFailed(
          query,"[" ⊹ errors.map(_.toString).intercalate(", ") ⊹ "]").left,
        qr.allRows.asScala.toVector.right)
    }

  def queryData(bucket: Bucket, query: String): Task[FileSystemError \/ Vector[Data]] =
    this.query(bucket, query) ∘ (_ >>= (_.traverse(rowToData)))

  def resultsFromCursor(cursor: Cursor): (Cursor, Vector[Data]) =
    (Cursor(Vector.empty), cursor.result)

  def rowToData(row: N1qlQueryRow): FileSystemError \/ Data = {
    val rowStr = new String(row.byteValue)

    DataCodec.parse(rowStr)(DataCodec.Precise).leftMap(err =>
      FileSystemError.readFailed(rowStr, err.shows))
  }
}
