/*
 * Copyright 2020 Precog Data
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

package quasar.api

import scala.{Int, Product, Serializable, StringContext}
import java.lang.String

import cats.{Eq, Show}

/* A single segment of a path into a data structure. */
sealed trait DataPathSegment extends Product with Serializable

object DataPathSegment {
  final case class Field(name: String) extends DataPathSegment
  case object AllFields extends DataPathSegment
  final case class Index(value: Int) extends DataPathSegment
  case object AllIndices extends DataPathSegment

  implicit val dataPathSegmentEq: Eq[DataPathSegment] =
    Eq instance { (x, y) =>
      x match {
        case Field(l) => y match {
          case Field(r) => l == r
          case _ => false
        }

        case AllFields => y match {
          case AllFields => true
          case _ => false
        }

        case Index(l) => y match {
          case Index(r) => l == r
          case _ => false
        }

        case AllIndices => y match {
          case AllIndices => true
          case _ => false
        }
      }
    }

  implicit val dataPathSegmentShow: Show[DataPathSegment] =
    Show show {
      case Field(n) => s".$n"
      case AllFields => "{*}"
      case Index(i) => s"[$i]"
      case AllIndices => "[*]"
    }
}
