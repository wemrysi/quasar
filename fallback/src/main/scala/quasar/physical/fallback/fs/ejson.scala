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

package quasar.physical.fallback.fs

import quasar.Predef._
import matryoshka._, Recursive.ops._
import quasar.{ ejson => ej }
import monocle._

trait EJsonAlgebra[A] {
  type Metadata

  val Null: A
  val Bool: Prism[A, Boolean]
  val Int: Prism[A, BigInt]
  val Dec: Prism[A, BigDecimal]
  val Str: Prism[A, String]
  val Obj: Prism[A, Map[String, A]]
  val Arr: Prism[A, Seq[A]]
  val Meta: Prism[A, Metadata]
  val Annotated: Prism[A, Metadata -> A]

  def fromCommon: Algebra[ej.Common, A] = {
    case ej.Null()  => Null
    case ej.Arr(x)  => Arr(x)
    case ej.Bool(x) => Bool(x)
    case ej.Str(x)  => Str(x)
    case ej.Dec(x)  => Dec(x)
  }
  def fromObj: Algebra[ej.Obj, A] = {
    case ej.Obj(xs) => Obj(xs)
  }
  /**
  def fromExtension: Algebra[ej.Extension, A] = {
    case ej.Meta(x, Meta(m)) => Annotated(m -> x)
    case ej.Map(x)           => Obj((x map { case Str(k) -> v => k -> v }).toMap)
    case ej.Int(x)           => Int(x)
    case ej.Byte(x)          => Int(x)
    case ej.Char(x)          => Str(x.toString)
  }
  **/

  def fromJson: Algebra[ej.Json, A] = _.run.fold(fromObj, fromCommon)

  def convert[T[_[_]] : Recursive](data: T[ej.Json]): A               = data cata fromJson
  def parser[T[_[_]]: Corecursive]: jawn.SupportParser[T[ej.Json]]    = ej.jsonParser[T, ej.Json]
  def ingest[T[_[_]] : Recursive : Corecursive](json: String): Try[A] = parser[T].parseFromString(json) map convert[T]
}

object EJsonAlgebra {
  import blueeyes.json._

  implicit object JValue extends EJsonAlgebra[JValue] {
    type Metadata = Unit
    private def mk[R](pf: PartialFunction[JValue, R])(g: R => JValue): Prism[JValue, R] = Prism.partial(pf)(g)

    val Null      = JNull
    val Bool      = mk { case JBool(x) => x } { JBool(_) }
    val Dec       = mk { case JNum(x) => x } { JNum(_) }
    val Str       = mk { case JString(x) => x } { JString(_) }
    val Obj       = mk { case JObject(x) => x } { JObject(_) }
    val Arr       = mk { case JArray(xs) => xs.toSeq } { xs => JArray(xs.toList) }
    val Int       = Prism[JValue, BigInt](_ => None)(x => JNum(BigDecimal(x.toString)))
    val Meta      = Prism[JValue, Metadata](_ => None)(_ => JUndefined)
    val Annotated = Prism[JValue, Metadata -> JValue](_ => None)(_._2)
  }
}
