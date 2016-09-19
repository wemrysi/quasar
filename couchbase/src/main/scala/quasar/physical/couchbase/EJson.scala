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

package quasar.physical.couchbase

import quasar.Predef._
import quasar.ejson, ejson._
import quasar.Planner.PlannerError

import argonaut.Argonaut._
import matryoshka._
import scalaz._, Scalaz._

object EJson {

  def fromCommon: Algebra[Common, String] = {
    case ejson.Arr(v)  => v.mkString("[", ", ", "]")
    case ejson.Null()  => "null"
    case ejson.Bool(v) => v.shows
    case ejson.Str(v)  => v
    case ejson.Dec(v)  => v.shows
  }

  def fromExtension: AlgebraM[PlannerError \/ ?, Extension, String] = {
    case ejson.Meta(v, meta) => ???
    case ejson.Map(v)        => v.toMap.asJson.nospaces.right
    case ejson.Byte(v)       => ???
    case ejson.Char(v)       => ???
    case ejson.Int(v)        => v.shows.right
  }

  val fromEJson: AlgebraM[PlannerError \/ ?, EJson, String] =
    _.run.fold(fromExtension, fromCommon(_).right)

}
