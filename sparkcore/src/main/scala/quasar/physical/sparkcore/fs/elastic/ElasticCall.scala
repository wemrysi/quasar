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

sealed trait ElasticCall[A]
final case class TypeExists(index: String, typ: String) extends ElasticCall[Boolean]
final case class ListTypes(index: String) extends ElasticCall[List[String]]
final case class ListIndeces() extends ElasticCall[List[String]]

object ElasticCall {

  class Ops[S[_]](implicit S: ElasticCall :<: S) {
    def typeExists(index: String, typ: String): Free[S, Boolean] = lift(TypeExists(index, typ)).into[S]
    def listTypes(index: String): Free[S, List[String]] = lift(ListTypes(index)).into[S]
    def listIndeces: Free[S, List[String]] = lift(ListIndeces()).into[S]
  }

  object Ops {
    implicit def apply[S[_]](implicit S: ElasticCall :<: S): Ops[S] = new Ops[S]
  }

  implicit def interpreter(sc: SparkContext): ElasticCall ~> Task = new (ElasticCall ~> Task) {

    def apply[A](from: ElasticCall[A]) = from match {
      case TypeExists(index, typ) =>
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
    }
  }


}
