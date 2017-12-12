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

package quasar.physical.rdbms.fs.postgres

import slamdata.Predef._
import quasar.Data
import quasar.physical.rdbms.planner.sql.SqlExpr
import quasar.physical.rdbms.planner.sql.SqlExpr.{Constant, Refs, Lt, Lte, Gt, Gte, Neq, Eq}

import matryoshka._
import matryoshka.implicits._

package object planner {

  object RelationalOperations {

    /**
      * Adds quotes around string constants when comparing to json string fields. See subselectFilter.test
      */
    def processQuotes[T[_[_]]: BirecursiveT](in: T[SqlExpr]): T[SqlExpr] = {

      object JsonRefs {
        def unapply(expr: T[SqlExpr]): Boolean =
          expr.project match {
            case Refs(elems) => elems.length > 2
            case _ => false
          }
      }

      def quotedStr(a: T[SqlExpr]): T[SqlExpr] =
        a.project match {
          case Constant(Data.Str(v)) =>
            Constant[T[SqlExpr]](Data.Str(s""""$v"""")).embed
          case other => other.embed
        }

      in.project match {
        case e@Eq(JsonRefs(), a2) =>
          e.copy(a2 = quotedStr(a2)).embed
        case e@Eq(a1, JsonRefs()) =>
          e.copy(a1 = quotedStr(a1)).embed

        case e@Neq(JsonRefs(), a2) =>
          e.copy(a2 = quotedStr(a2)).embed
        case e@Neq(a1, JsonRefs()) =>
          e.copy(a1 = quotedStr(a1)).embed

        case e@Gt(JsonRefs(), a2) =>
          e.copy(a2 = quotedStr(a2)).embed
        case e@Gt(a1, JsonRefs()) =>
          e.copy(a1 = quotedStr(a1)).embed

        case e@Gte(JsonRefs(), a2) =>
          e.copy(a2 = quotedStr(a2)).embed
        case e@Gte(a1, JsonRefs()) =>
          e.copy(a1 = quotedStr(a1)).embed

        case e@Lt(JsonRefs(), a2) =>
          e.copy(a2 = quotedStr(a2)).embed
        case e@Lt(a1, JsonRefs()) =>
          e.copy(a1 = quotedStr(a1)).embed

        case e@Lte(JsonRefs(), a2) =>
          e.copy(a2 = quotedStr(a2)).embed
        case e@Lte(a1, JsonRefs()) =>
          e.copy(a1 = quotedStr(a1)).embed
        case other =>
          other.embed
      }
    }
  }
}
