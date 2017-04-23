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

package quasar.physical.sparkcore.fs.elastic

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.physical.sparkcore.fs.queryfile.Input
import quasar.contrib.pathy._
// import quasar.effect._
import quasar.fs.FileSystemError
import quasar.fs.FileSystemErrT
// import quasar.fs.FileSystemError._
// import quasar.fs.PathError._
import quasar.fp.free._
import quasar.fp.ski._
import quasar.contrib.pathy._

import org.elasticsearch.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO_ES use ElasticCall.Ops instead of lift().into[S]
object queryfile {

  private def path(file: AFile) = posixCodec.unsafePrintPath(file) // TODO_ES handle invalid paths
  private def parseIndex(adir: ADir) = posixCodec.unsafePrintPath(adir).replace("/", "") // TODO_ES handle invalid paths

  def fromFile(sc: SparkContext, file: AFile): Task[RDD[Data]] = Task.delay {
    sc.esJsonRDD(path(file)).map(_._2).map(raw => DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι))
  }

  def store[S[_]](rdd: RDD[Data], out: AFile)(implicit
    S: Task :<: S
  ): Free[S, Unit] = lift(Task.delay {
    rdd.flatMap(DataCodec.render(_)(DataCodec.Precise).toList)
      .saveJsonToEs(path(out))
  }).into[S]

  def fileExists[S[_]](f: AFile)(implicit
    E: ElasticCall :<: S
  ): Free[S, Boolean] = {
    val index :: typ :: Nil = path(f).split("/").toList // TODO_ES handle invalid paths
    lift(TypeExists(index, typ)).into[S]
  }

  def listContents[S[_]](adir: ADir)(implicit
    E: ElasticCall :<: S
  ): FileSystemErrT[Free[S, ?], Set[PathSegment]] = {
    val segments = if(adir === rootDir) {
      lift(ListIndecies()).into[S].map(_.map(t => DirName(t).left[FileName]).toSet)
    } else {
      lift(ListTypes(parseIndex(adir))).into[S].map(_.map(t => FileName(t).right[DirName]).toSet)
    }
    EitherT(segments.map(_.right[FileSystemError]))
  }

  def readChunkSize: Int = 5000

  def input[S[_]](implicit
    s0: Task :<: S,
    s1: ElasticCall :<: S
  ): Input[S] =
    Input[S](fromFile _, store[S] _, fileExists[S] _, listContents[S] _, readChunkSize _)
}
