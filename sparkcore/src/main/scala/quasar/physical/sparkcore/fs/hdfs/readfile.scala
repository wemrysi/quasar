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

package quasar.physical.sparkcore.fs.hdfs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.effect.Read
import quasar.fp.free._
import quasar.physical.sparkcore.fs.readfile.{Offset, Limit}
import quasar.physical.sparkcore.fs.readfile.Input

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import pathy.Path.posixCodec
import scalaz._
import scalaz.concurrent.Task

class readfile(host: String, port: Int) {

  val prefix = s"hdfs://$host:$port"

  def hdfsPath(f: AFile) = prefix + posixCodec.unsafePrintPath(f)

  def rddFrom[S[_]](f: AFile, offset: Offset, maybeLimit: Limit)
    (implicit read: Read.Ops[SparkContext, S]): Free[S, RDD[String]] =
    read.asks { sc =>
      sc.textFile(hdfsPath(f))
        .zipWithIndex()
        .filter {
        case (value, index) =>
          maybeLimit.fold(
            index >= offset.get
          ) (
            limit => index >= offset.get && index < limit.get + offset.get
          )
      }.map{
        case (value, index) => value
      }
      
    }

  def fileExists[S[_]](f: AFile)(implicit s0: Task :<: S): Free[S, Boolean] =
    lift(Task.delay {
      val conf: Configuration = new Configuration()
      val uri: URI = new URI(hdfsPath(f));
      val hdfs: FileSystem= FileSystem.get(uri, conf);
      val path: Path = new Path(uri)
      hdfs.exists(path)
    }).into[S]

  // TODO arbitrary value, more or less a good starting point
  // but we should consider some measuring
  def readChunkSize: Int = 5000

  def input[S[_]](implicit read: Read.Ops[SparkContext, S], s0: Task :<: S) =
    Input((f,off, lim) => rddFrom(f, off, lim), f => fileExists(f), readChunkSize _)

}
