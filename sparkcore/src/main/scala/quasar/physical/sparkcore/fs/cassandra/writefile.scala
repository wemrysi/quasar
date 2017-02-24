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

package quasar.physical.sparkcore.fs.cassandra

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.{Data, DataCodec}
import quasar.effect._
import quasar.fs._, WriteFile._, FileSystemError._
import quasar.fp.free._

import org.apache.spark.SparkContext
import com.datastax.spark.connector.cql.CassandraConnector
import pathy.Path.fileParent
import scalaz._, Scalaz._

object writefile {

  import common._

  def chrooted[S[_]](prefix: ADir)(implicit
    s0: KeyValueStore.Ops[WriteHandle, AFile, S],
    s1: MonotonicSeq :<: S,
    s2: Read.Ops[SparkContext, S]
  ) : WriteFile ~> Free[S, ?] =
    flatMapSNT(interpret) compose chroot.writeFile[WriteFile](prefix)

  def interpret[S[_]](implicit 
    s0: KeyValueStore.Ops[WriteHandle, AFile, S],
    s1: MonotonicSeq :<: S,
    s2: Read.Ops[SparkContext, S]
    ): WriteFile ~> Free[S, ?] = {
    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wr: WriteFile[A]): Free[S, A] = wr match {
        case Open(f) => open(f)
        case Write(h, ch) => write(h, ch)
        case Close(h) => close(h)
      }
    }
  }

  def open[S[_]](file: AFile)(implicit 
    writers: KeyValueStore.Ops[WriteHandle, AFile, S],
    gen: MonotonicSeq.Ops[S],
    s1: Read.Ops[SparkContext, S]
    ): Free[S, FileSystemError \/ WriteHandle] = 
    for {
      id <- gen.next
      _ <- create(file)
      wh = WriteHandle(file, id)
      _ <- writers.put(wh, file)
    } yield (wh.right)

  def create[S[_]](file: AFile)(implicit
    read: Read.Ops[SparkContext, S]
    ): Free[S, Unit] = 
    read.asks { sc =>
      val connector = CassandraConnector(sc.getConf)
      connector.withSessionDo { implicit session =>
        val k = if(!keyspaceExists(keyspace(fileParent(file)))) {
          createKeyspace(keyspace(fileParent(file)))
        }

        val t = if(!tableExists(keyspace(fileParent(file)), tableName(file)))
          createTable(keyspace(fileParent(file)), tableName(file))
      }
    }

  def write[S[_]](handle: WriteHandle, chunks: Vector[Data])(implicit
    writers: KeyValueStore.Ops[WriteHandle, AFile, S],
    read: Read.Ops[SparkContext, S]
  ): Free[S, Vector[FileSystemError]] = for {
    maybeAFile <- writers.get(handle).run
    vectors <- read.asks {
      sc =>

      implicit val codec: DataCodec = DataCodec.Precise

      def _write(file: AFile) : Vector[FileSystemError] = {
        val textChunk: Vector[(String, Data)] =
          chunks.map(d => DataCodec.render(d) strengthR d).unite
      
        CassandraConnector(sc.getConf).withSessionDo {implicit session =>
          if(!tableExists(keyspace(fileParent(file)), tableName(file))) {
            Vector(unknownWriteHandle(handle))
          } else {
            textChunk.flatMap {
              case (text,data) => 
                \/.fromTryCatchNonFatal{
                  insertData(keyspace(fileParent(handle.file)), tableName(handle.file), text)
                }.fold(
                  ex => Vector(writeFailed(data, ex.getMessage)),
                  u => Vector.empty[FileSystemError]
                )
            }
          }
        }
      }

      maybeAFile.map(_write).getOrElse(
        Vector[FileSystemError](unknownWriteHandle(handle))
      )
    }
  } yield vectors

  def close[S[_]](handle: WriteHandle)(implicit
    writers: KeyValueStore.Ops[WriteHandle, AFile, S]
  ): Free[S, Unit] = 
    writers.delete(handle)
}
