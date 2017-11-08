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

package quasar.ejson

import slamdata.Predef.{Int => _, Map => _, _}
import quasar.contrib.matryoshka.totally

import matryoshka._
import matryoshka.implicits._
import scalaz.Coproduct
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.bind._
import scalaz.syntax.traverse._

object EJson {
  def fromJson[A](f: String => A): Json[A] => EJson[A] =
    json => Coproduct(json.run.leftMap(Extension.fromObj(f)))

  def fromCommon[T](c: Common[T])(implicit T: Corecursive.Aux[T, EJson]): T =
    CommonEJson(c).embed

  def fromExt[T](e: Extension[T])(implicit T: Corecursive.Aux[T, EJson]): T =
    ExtEJson(e).embed

  def arr[T[_[_]]](xs: T[EJson]*)(implicit T: CorecursiveT[T]): T[EJson] =
    fromCommon(Arr(xs.toList))

  def bool[T[_[_]]](b: Boolean)(implicit T: CorecursiveT[T]): T[EJson] =
    fromCommon(Bool(b))

  def dec[T[_[_]]](d: BigDecimal)(implicit T: CorecursiveT[T]): T[EJson] =
    fromCommon(Dec(d))

  def nul[T[_[_]]](implicit T: CorecursiveT[T]): T[EJson] =
    fromCommon(Null())

  def str[T[_[_]]](s: String)(implicit T: CorecursiveT[T]): T[EJson] =
    fromCommon(Str(s))

  def int[T[_[_]]](d: BigInt)(implicit T: CorecursiveT[T]): T[EJson] =
    fromExt(Int(d))

  def obj[T[_[_]]](xs: (String, T[EJson])*)(implicit T: CorecursiveT[T]): T[EJson] =
    map((xs.map { case (s, t) => str[T](s) -> t }): _*)

  def map[T[_[_]]](xs: (T[EJson], T[EJson])*)(implicit T: CorecursiveT[T]): T[EJson] =
    fromExt(Map(xs.toList))

  def isNull[T](ej: T)(implicit T: Recursive.Aux[T, EJson]): Boolean =
    CommonEJson.prj(ej.project) exists (quasar.ejson.nul.nonEmpty(_))

  /** Replaces `Meta` nodes with their value component. */
  def elideMetadata[T](
    implicit T: Recursive.Aux[T, EJson]
  ): EJson[T] => EJson[T] = totally {
    case ExtEJson(Meta(v, _)) => v.project
  }

  /** Replace a string with an array of characters. */
  def replaceString[T](
    implicit T: Corecursive.Aux[T, EJson]
  ): EJson[T] => EJson[T] = totally {
    case CommonEJson(Str(s)) =>
      CommonEJson(quasar.ejson.arr[T](s.toList map (c => fromExt(quasar.ejson.char[T](c)))))
  }

  /** Replace an array of characters with a string. */
  def restoreString[T](
    implicit
    TC: Corecursive.Aux[T, EJson],
    TR: Recursive.Aux[T, EJson]
  ): EJson[T] => EJson[T] = totally {
    case a @ CommonEJson(Arr(t :: ts)) =>
      (t :: ts).traverse(t => ExtEJson.prj(t.project) >>= (quasar.ejson.char[T].getOption(_)))
        .fold(a)(cs => CommonEJson(quasar.ejson.str[T](cs.mkString)))
  }
}
