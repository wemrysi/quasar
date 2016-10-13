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

package quasar.physical.couchbase.planner

import quasar.Predef._
import quasar.contrib.matryoshka._
import quasar.fp._, eitherT._, ski.κ
import quasar.NameGenerator
import quasar.PhaseResult.Detail
import quasar.physical.couchbase._
import quasar.physical.couchbase.N1QL._, Select._
import quasar.Planner.{NonRepresentableEJson, PlannerError}
import quasar.qscript, qscript._
import quasar.std.StdLib.string._

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz.{ToIdOps => _, _}

final class MapFuncPlanner[F[_]: Monad: NameGenerator, T[_[_]]: Recursive: ShowT]
  extends Planner[F, MapFunc[T, ?]] {
  import MapFuncs._

  def plan: AlgebraM[M, MapFunc[T, ?], N1QL] = {
    // nullary
    case Constant(v) =>
      EitherT(
        v.cataM(EJson.fromEJson).bimap[PlannerError, N1QL](
          κ(NonRepresentableEJson(v.shows)),
          PartialQueryString(_)
        ).point[PR])
    case Undefined() =>
      partialQueryString("null").point[M]

    // array
    case Length(a1) =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"array_length($a1N1ql)")
      )

    // date
    case Date(a1) =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"""
          (case
           when regexp_contains("$a1N1ql", "$dateRegex") then v
           else null
           end)""")
     )
    case Time(a1) =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"""
          (case
           when regexp_contains("$a1N1ql", "$timeRegex") then v
           else null
           end)""")
     )
    case Timestamp(a1) =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"""
          (case
           when regexp_contains("$a1N1ql", "$timestampRegex") then v
           else null
           end)""")
       )
    case Interval(a1) => ???
    case TimeOfDay(a1) => ???
    case ToTimestamp(a1) => ???
    case ExtractCentury(a1) => ???
    case ExtractDayOfMonth(a1) => ???
    case ExtractDecade(a1) => ???
    case ExtractDayOfWeek(a1) => ???
    case ExtractDayOfYear(a1) => ???
    case ExtractEpoch(a1) => ???
    case ExtractHour(a1) => ???
    case ExtractIsoDayOfWeek(a1) => ???
    case ExtractIsoYear(a1) => ???
    case ExtractMicroseconds(a1) => ???
    case ExtractMillennium(a1) => ???
    case ExtractMilliseconds(a1) => ???
    case ExtractMinute(a1) => ???
    case ExtractMonth(a1) => ???
    case ExtractQuarter(a1) => ???
    case ExtractSecond(a1) => ???
    case ExtractTimezone(a1) => ???
    case ExtractTimezoneHour(a1) => ???
    case ExtractTimezoneMinute(a1) => ???
    case ExtractWeek(a1) => ???
    case ExtractYear(a1) => ???
    case Now() =>
      partialQueryString("now_str()").point[M]

    // math
    case Negate(a1)       =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"-$a1N1ql)")
      )
    case Add(a1, a2)      =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql + $a2N1ql)")
      )
    case Multiply(a1, a2) =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql * $a2N1ql)")
      )
    case Subtract(a1, a2) =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql - $a2N1ql)")
      )
    case Divide(a1, a2)   =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql / $a2N1ql)")
      )
    case Modulo(a1, a2)   =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql % $a2N1ql)")
      )
    case Power(a1, a2)    =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"power($a1N1ql, $a2N1ql)")
      )

    // relations
    case Not(a1)             =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"not $a1N1ql)")
      )
    case Eq(a1, a2)          =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql = $a2N1ql)")
      )
    case Neq(a1, a2)         =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql != $a2N1ql)")
      )
    case Lt(a1, a2)          =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql < $a2N1ql)")
      )
    case Lte(a1, a2)         =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql <= $a2N1ql)")
      )
    case Gt(a1, a2)          =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql > $a2N1ql)")
      )
    case Gte(a1, a2)         =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql >= $a2N1ql)")
      )
    case IfUndefined(a1, a2) => ???
    case And(a1, a2)         =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql and $a2N1ql)")
      )
    case Or(a1, a2)          =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"($a1N1ql or $a2N1ql)")
      )
    case Coalesce(a1, a2)    => ???
    case Between(a1, a2, a3) =>
      for {
        t      <- genName[M]
        a1N1ql <- n1ql[M](a1)
        a2N1ql <- n1ql[M](a2)
        a3N1ql <- n1ql[M](a3)
        s      =  select(
                    value         = true,
                    resultExprs   = t.wrapNel,
                    keyspace      = a1,
                    keyspaceAlias = t) |>
                  filter.set(s"$t between $a2N1ql and $a3N1ql".some)
        sN1ql  <- n1ql[M](s)
        _      <- prtell[M](Vector(Detail(
                    "N1QL Between",
                    s"""  a1:   $a1N1ql
                       |  a2:   $a2N1ql
                       |  a3:   $a3N1ql
                       |  n1ql: $sN1ql""".stripMargin('|'))))
      } yield s
    case Cond(cond, then_, else_) =>
      (n1ql[M](cond) |@| n1ql[M](then_) |@| n1ql[M](else_))((condN1ql, thenN1ql, elseN1ql) =>
        partialQueryString(s"""
          (case
           when $condN1ql then $thenN1ql
           else $elseN1ql
           end)""")
      )

    // set
    case Within(a1, a2) =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"array_contains($a2N1ql, $a1N1ql)")
      )

    // string
    case Lower(a1)             =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"lower($a1N1ql)")
      )
    case Upper(a1)             =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"upper($a1N1ql)")
      )
    case Bool(a1)              =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"""
          (case
           when lower("$a1N1ql") = "true"  then true
           when lower("$a1N1ql") = "false" then false
           else null
           end)""")
      )
    // TODO: Handle large numbers across the board. Couchbase's number type truncates.
    case Integer(a1)           =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"""
          (case
           when tonumber("$a1N1ql") = floor(tonumber("$a1N1ql")) then tonumber("$a1N1ql")
           else null
           end)""")
      )
    case Decimal(a1)           =>
      n1ql[M](a1).map(a1N1ql =>
        partialQueryString(s"""tonumber("$a1N1ql")""")
      )
    case Null(a1)              =>
      n1ql[M](a1).map(a1N1ql =>
        // TODO: Undefined isn't available, what to use?
        partialQueryString(s"""
          (case
           when lower("$a1N1ql") = "null" then null
           else undefined
           end)""")
      )
    case ToString(a1)          =>
      n1ql[M](a1).map(a1N1ql =>
        // overly simplistic?
        partialQueryString(s"tostring($a1N1ql)")
      )
    case Search(a1, a2, a3)    =>
      (n1ql[M](a1) |@| n1ql[M](a2) |@| n1ql[M](a3))((a1N1ql, a2N1ql, a3N1ql) =>
        partialQueryString(s"""
          (case
           when $a3N1ql then regexp_contains("$a1N1ql", "(?i)" || "$a2N1ql")
           else regexp_contains("$a1N1ql", "$a2N1ql")
           end)""")
        )
    case Substring(a1, a2, a3) =>
      (n1ql[M](a1) |@| n1ql[M](a2) |@| n1ql[M](a3))((a1N1ql, a2N1ql, a3N1ql) =>
        partialQueryString(s"""substr("$a1N1ql", "$a2N1ql", "$a3N1ql")""")
      )

    // structural
    case MakeArray(a1)                            =>
      for {
        a1N1ql  <- n1ql[M](a1)
        n1qlStr =  s"[$a1N1ql]"
        _       <- prtell[M](Vector(Detail(
                     "N1QL MakeArray",
                     s"""  a1:   $a1N1ql
                        |  n1ql: $n1qlStr""".stripMargin('|'))))
      } yield partialQueryString(n1qlStr)
    case MakeMap(a1, a2)                          =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"""object_add({}, "$a1N1ql", $a2N1ql)""")
      )
    case ConcatArrays(a1, a2)                     =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"array_concat($a1N1ql, $a2N1ql)")
      )
    case ConcatMaps(a1, a2)                       =>
      for {
        a1N1ql  <- n1ql[M](a1)
        a2N1ql  <- n1ql[M](a2)
        _       <- prtell[M](Vector(Detail(
                     "N1QL ConcatMaps",
                     s"""  a1:   $a1N1ql
                        |  a2:   $a2N1ql
                        |  n1ql: ???""".stripMargin('|'))))
      } yield partialQueryString("???ConcatMaps???")
    case ProjectField(PartialQueryString(a1), a2) =>
      for {
        a2N1ql  <- n1ql[M](a2)
        n1qlStr =  s"$a1.$a2N1ql"
        _       <- prtell[M](Vector(Detail(
                     "N1QL ProjectField(PartialQueryString(_), _)",
                     s"""  a1:   $a1
                        |  a2:   $a2N1ql
                        |  n1ql: $n1qlStr""".stripMargin('|'))))
      } yield partialQueryString(n1qlStr)
    case ProjectField(a1, a2) =>
      for {
        tempName <- genName[M]
        a1N1ql   <- n1ql[M](a1)
        a2N1ql   <- n1ql[M](a2)
        s        =  select(
                      value         = true,
                      resultExprs   = a2N1ql.wrapNel,
                      keyspace      = a1,
                      keyspaceAlias = tempName)
        sN1ql    <- n1ql[M](s)
        _        <- prtell[M](Vector(Detail(
                      "N1QL ProjectField(_, _)",
                      s"""  a1:   $a1N1ql
                         |  a2:   $a2N1ql
                         |  n1ql: $sN1ql""".stripMargin('|'))))
        sN1ql    <- n1ql[M](s)
      } yield s
    case ProjectIndex(PartialQueryString(a1), a2) =>
      for {
        a2N1ql  <- n1ql[M](a2)
        n1qlStr =  s"$a1[$a2N1ql]"
        _       <- prtell[M](Vector(Detail(
                     "N1QL ProjectIndex(PartialQueryString(_), _)",
                     s"""  a1:   $a1
                        |  a2:   $a2N1ql
                        |  n1ql: $n1qlStr""".stripMargin('|'))))
      } yield partialQueryString(n1qlStr)
    case ProjectIndex(a1, a2)                     =>
      for {
        tmpName1 <- genName[M]
        tmpName2 <- genName[M]
        a1N1ql   <- n1ql[M](a1)
        a2N1ql   <- n1ql[M](a2)
                    // TODO: custom select for the moment
        n1qlStr  =  s"(select value $tmpName1[$a2N1ql] let $tmpName1 = (select value array_agg($tmpName2) from $a1N1ql $tmpName2))"
        _        <- prtell[M](Vector(Detail(
                      "N1QL ProjectIndex(_, _)",
                      s"""  a1:   $a1N1ql
                         |  a2:   $a2N1ql
                         |  n1ql: $n1qlStr""".stripMargin('|'))))
      } yield partialQueryString(n1qlStr)
    case DeleteField(a1, a2)                      =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"object_remove($a2N1ql, $a1N1ql)")
      )

    // helpers & QScript-specific
    case DupMapKeys(a1)                   => ???
    case DupArrayIndices(a1)              => ???
    case ZipMapKeys(a1)                   =>
      for {
        a1N1ql   <- n1ql[M](a1)
        _       <- prtell[M](Vector(Detail(
                     "N1QL ZipMapKeys",
                     s"""  a1:   $a1N1ql
                        |  n1ql: ???""".stripMargin('|'))))

      } yield partialQueryString(s"???ZipMapKeys???")
    case ZipArrayIndices(a1)              => ???
    case Range(a1, a2)                    =>
      (n1ql[M](a1) |@| n1ql[M](a2))((a1N1ql, a2N1ql) =>
        partialQueryString(s"[$a2N1ql:($a1N1ql + 1)]")
      )
    case Guard(expr, typ, cont, fallback) =>
      cont.point[M]
  }
}
