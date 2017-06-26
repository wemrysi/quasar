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
import quasar.fp.free._

import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.http.ElasticDsl._
import org.http4s.client.blaze._
import org.apache.spark._
import scalaz._, scalaz.concurrent.Task

final case class IndexType(index: String, typ: String)

object IndexType {
  implicit val ShowIndexType: Show[IndexType] = new Show[IndexType] {
    override def shows(it: IndexType): String = s"${it.index}/${it.typ}"
  }
}

sealed trait ElasticCall[A]
final case class CreateIndex(index: String) extends ElasticCall[Unit]
final case class Copy(src: IndexType, dst: IndexType) extends ElasticCall[Unit]
final case class TypeExists(indexType: IndexType) extends ElasticCall[Boolean]
final case class ListTypes(index: String) extends ElasticCall[List[String]]
final case class ListIndeces() extends ElasticCall[List[String]]
final case class DeleteIndex(index: String) extends ElasticCall[Unit]
final case class DeleteType(indexType: IndexType) extends ElasticCall[Unit]

object ElasticCall {

  class Ops[S[_]](implicit S: ElasticCall :<: S) {
    def createIndex(index: String): Free[S, Unit] = lift(CreateIndex(index)).into[S]
    def copy(src: IndexType, dst: IndexType): Free[S, Unit] = lift(Copy(src, dst)).into[S]
    def typeExists(indexType: IndexType): Free[S, Boolean] = lift(TypeExists(indexType)).into[S]
    def listTypes(index: String): Free[S, List[String]] = lift(ListTypes(index)).into[S]
    def listIndeces: Free[S, List[String]] = lift(ListIndeces()).into[S]
    def deleteIndex(index: String): Free[S, Unit] = lift(DeleteIndex(index)).into[S]
    def deleteType(indexType: IndexType): Free[S, Unit] = lift(DeleteType(indexType)).into[S]

    def indexExists(index: String): Free[S, Boolean] = listIndeces.map(_.contains(index))
  }
  
  object Ops {
    implicit def apply[S[_]](implicit S: ElasticCall :<: S): Ops[S] = new Ops[S]
  }

  implicit def interpreter(sc: SparkContext): ElasticCall ~> Task = new (ElasticCall ~> Task) {

    def apply[A](from: ElasticCall[A]) = from match {
      case CreateIndex(index) => Task.delay {
        // TODO_ES
      }
      case Copy(src, dst) => Task.delay {
        // TODO_ES
      }
      case TypeExists(IndexType(index, typ)) =>
        Task.delay {
          val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
          val result = client.execute { typesExist(typ) in index }.await.exists
          client.close()
          result
        }
      case ListTypes(index) => for {
        httpClient <- Task.delay {
          PooledHttp1Client()
        }
        indices <- httpClient.expect[String]("http://localhost:9200/_mapping?")
      } yield {
        val result = DataCodec.parse(indices)(DataCodec.Precise).fold(error => List.empty[String], {
          case Data.Obj(idxs) => idxs(index) match {
            case Data.Obj(mapping) => mapping("mappings") match {
              case Data.Obj(types) => types.keys.toList.map(d => s"$d")
              case _ => List.empty[String] // TODO_ES handling errors
            }
            case _ => List.empty[String] // TODO_ES handling errors
          }
          case _ => List.empty[String] // TODO_ES handling errors
        })
        httpClient.shutdownNow() // TODO_ES handling resources
        result
      }
      case ListIndeces() => for {
        httpClient <- Task.delay {
          PooledHttp1Client()
        }
        indices <- httpClient.expect[String]("http://localhost:9200/_aliases")
      } yield {
        val result = DataCodec.parse(indices)(DataCodec.Precise).fold(error => List.empty[String], {
          case Data.Obj(m) => m.keys.toList.map(d => s"$d")
          case _ => List.empty[String] // TODO_ES handling errors
        })
        httpClient.shutdownNow() // TODO_ES handling resources
        result
      }
      case DeleteIndex(index) => Task.delay {
        // TODO_ES
        ()
      }
      case DeleteType(IndexType(index, typ)) => Task.delay {
        // TODO_ES
        ()
      }
    }
  }


}
