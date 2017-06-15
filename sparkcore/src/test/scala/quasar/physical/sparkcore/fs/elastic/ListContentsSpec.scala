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

import pathy._, Path._
import scalaz._, Scalaz._
import org.specs2.scalaz.DisjunctionMatchers

class ListContentsSpec extends quasar.Qspec with DisjunctionMatchers {

  def elasticInterpreter(indices: List[String]): ElasticCall ~> Id = new (ElasticCall ~> Id) {
    def apply[A](from: ElasticCall[A]) = from match {
      case TypeExists(index, typ) => true
      case ListTypes(index) => List.empty[String]
      case ListIndeces() => indices
    }
  }


  "ListContents" should {
    "for a /" should {
      "list all indexes if there are no multi-level folders" in {
        val indices = List("foo", "bar", "baz")
        val program = queryfile.listContents[ElasticCall](rootDir).run
        val result = program.foldMap(elasticInterpreter(indices)).map(_.toList)

        result must be_\/-(indices.map(i => DirName(i).left[FileName]))
      }

      "list all indexes if there are multi-level folders" in {
        val indices = List(s"foo${separator}bar", "baz", s"muu${separator}nuu${separator}buu")
        val program = queryfile.listContents[ElasticCall](rootDir).run
        val result = program.foldMap(elasticInterpreter(indices)).map(_.toList)

        result must be_\/-(List(DirName("foo"), DirName("baz"), DirName("muu")).map(_.left[FileName]))
      }
    }
  }
}
