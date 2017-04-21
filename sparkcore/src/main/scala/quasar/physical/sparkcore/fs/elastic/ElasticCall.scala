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

import org.apache.spark._
import scalaz._, scalaz.concurrent.Task

sealed trait ElasticCall[A]
final case class TypeExists(index: String, typ: String) extends ElasticCall[Boolean]
final case class ListTypes(index: String) extends ElasticCall[List[String]]


object ElasticCall {

  implicit def interpreter(sc: SparkContext): ElasticCall ~> Task = new (ElasticCall ~> Task) {
    def apply[A](from: ElasticCall[A]) = from match {
      case TypeExists(index, typ) => Task.delay {
        false // ES_TODO
      }
      case ListTypes(index) => Task.delay {
        List.empty[String] //ES_TODO
      }
    }
  }


}
