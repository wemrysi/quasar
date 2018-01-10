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

package quasar.physical.rdbms.planner.sql

import matryoshka.BirecursiveT
import slamdata.Predef._
import matryoshka.implicits._

import scalaz._
import Scalaz._

object Metas {

  sealed trait MetaType
  final case object Dot extends MetaType
  final case object Arr extends MetaType

  sealed trait Meta

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  final case class Branch(m: String => (MetaType, Meta), debug: String) extends Meta

  implicit val metaShow: Show[Meta] = {
    Show.shows[Meta] {
      case Branch(_, debug) => debug
    }
  }

  val mArr: Meta = Branch((_: String) => (Arr, mArr), "mArr")
  val Default = Branch((_: String) => (Dot, mArr), "Default")

  import SqlExpr._

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def deriveMeta[T[_[_]]: BirecursiveT](expr: T[SqlExpr]): Meta = {
      println(s"deriveMeta for ${expr.project}")
      val meta = expr.project match {
        case SqlExpr.Id(_, m) =>
          println(s"meta for id")
          m
        case ExprPair(_, _, m) =>
          println(s"meta for ExprPair")
          m
        case ExprWithAlias(v, _) =>
          println(s"meta for ExprWithAlias")
          deriveMeta(v)
        case Select(Selection(_, _, m), _, _, _, _, _) =>
          println(s"meta for Select")
          m
        case _ =>
          println(s"Default meta")
          Default
      }
      println(meta.shows)
      meta
    }
}


