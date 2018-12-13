/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.qscript.rewrites

import slamdata.Predef.{None, Option, Set, Some, String}

import matryoshka.{BirecursiveT, Embed}
import matryoshka.data.free._
import matryoshka.implicits._

import quasar.common.{CPath, CPathField}
import quasar.contrib.iota._
import quasar.contrib.scalaz.free._
import quasar.ejson.{CommonEJson, Str}
import quasar.qscript.FreeMapA
import quasar.qscript.MapFuncCore.StaticMap

import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._

object MaskedObject {
  def unapply[T[_[_]]: BirecursiveT](prjd: FreeMapA[T, CPath]): Option[Set[String]] =
    StaticMap.unapply(prjd.project).flatMap(_.foldLeftM(Set[String]()) {
      case (s, (Embed(CommonEJson(Str(key))), FreeA(CPath(CPathField(field)))))
          if key === field =>
        Some(s + field)

      case _ => None
    })
}
