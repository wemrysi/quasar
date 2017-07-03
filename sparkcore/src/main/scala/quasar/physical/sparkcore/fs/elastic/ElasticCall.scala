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
import quasar.fp.free._

import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.http.ElasticDsl._
import scalaz._, Scalaz._, scalaz.concurrent.Task

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
final case class ListIndices() extends ElasticCall[List[String]]
final case class DeleteIndex(index: String) extends ElasticCall[Unit]
final case class DeleteType(indexType: IndexType) extends ElasticCall[Unit]
final case class IndexInto(indexType: IndexType, data: List[(String, String)]) extends ElasticCall[Unit]

object ElasticCall {

  class Ops[S[_]](implicit S: ElasticCall :<: S) {
    def createIndex(index: String): Free[S, Unit] = lift(CreateIndex(index)).into[S]
    def copy(src: IndexType, dst: IndexType): Free[S, Unit] = lift(Copy(src, dst)).into[S]
    def typeExists(indexType: IndexType): Free[S, Boolean] = lift(TypeExists(indexType)).into[S]
    def listTypes(index: String): Free[S, List[String]] = lift(ListTypes(index)).into[S]
    def listIndices: Free[S, List[String]] = lift(ListIndices()).into[S]
    def deleteIndex(index: String): Free[S, Unit] = lift(DeleteIndex(index)).into[S]
    def deleteType(indexType: IndexType): Free[S, Unit] = lift(DeleteType(indexType)).into[S]
    def indexInto(indexType: IndexType, data: List[(String, String)]): Free[S, Unit] = lift(IndexInto(indexType, data)).into[S]

    def indexExists(index: String): Free[S, Boolean] = listIndices.map(_.contains(index))
  }
  
  object Ops {
    implicit def apply[S[_]](implicit S: ElasticCall :<: S): Ops[S] = new Ops[S]
  }


  //TODO_ES reuse single client per interpreter, close when unmounted
  implicit def interpreter: ElasticCall ~> Task = new (ElasticCall ~> Task) {

    def _listIndices: Task[List[String]] = Task.delay {
      val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
      val result = client.execute { catIndices() }.await
      client.close()
      result.map(_.index).toList
    }

    def _create(index: String): Task[Unit] = for {
      indices <- _listIndices
      _       <- if(indices.contains(index)) ().point[Task] else Task.delay {
        val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
        val result = client.execute { createIndex(index) }.await
        client.close()
        ()
      }
    } yield ()

    def _delete(index: String): Task[Unit] = for {
      indices <- _listIndices
      _       <- if(!indices.contains(index)) ().point[Task] else Task.delay {
        val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
        val result = client.execute { deleteIndex(index) }.await
        client.close()
        ()
      }
    } yield ()

    def _indexInto(indexType: IndexType, data: List[(String, String)]): Task[Unit] = Task.delay {
        val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
        val result = client.execute { indexInto(indexType.index / indexType.typ) fields (data:_*) }.await
        client.close()
        ()
    }

    def _typeExists(index: String, typ: String): Task[Boolean] = Task.delay {
        val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
        val result = client.execute { typesExist(typ) in index }.await.exists
        client.close()
        result
    }

    def _listTypes(index: String): Task[List[String]] = for {
      indices <- _listIndices
      types   <- if(!indices.contains(index)) List.empty.point[Task] else Task.delay {
        val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
        val result = client.execute { getMapping(index) }.await
        client.close()
        result.toList.flatMap(_.mappings.keys.toList)
      }
    } yield types

    def apply[A](from: ElasticCall[A]) = from match {
      case CreateIndex(index) => _create(index)
      case IndexInto(indexType: IndexType, data: List[(String, String)]) => _indexInto(indexType, data)
      case Copy(src, dst) => Task.delay {
        // TODO_ES
      }
      case TypeExists(IndexType(index, typ)) => _typeExists(index, typ)
      case ListTypes(index) => _listTypes(index)
      case ListIndices() => _listIndices
      case DeleteIndex(index) => _delete(index)
      case DeleteType(IndexType(index, typ)) => Task.delay {
        // TODO_ES
        ()
      }
    }
  }


}
