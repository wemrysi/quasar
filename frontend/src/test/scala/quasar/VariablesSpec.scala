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

package quasar

import slamdata.Predef._
import quasar.sql._

import matryoshka.data.Fix
import scalaz._, Scalaz._

class VariablesSpec extends quasar.Qspec {

  def parse(query: String): Fix[Sql] =
    fixParser.parseExpr(Query(query)).map(_.makeTables(Nil)).toOption.get

  "Variables" >> {
    "subtsVars" >> {
      "list all missing variables" >> {
        "all are missing" >> {
          Variables.substVars(parse("select * from :foo where :baz"), Variables.empty) must_===
            NonEmptyList(
              SemanticError.unboundVariable(VarName("foo")),
              SemanticError.unboundVariable(VarName("baz"))).left
        }
        "some are missing" >> {
          val vars = Variables.fromMap(Map("baz" -> "age = 7"))
          Variables.substVars(parse("select :biz from :foo where :baz"), vars) must_===
            NonEmptyList(
              SemanticError.unboundVariable(VarName("foo")),
              SemanticError.unboundVariable(VarName("biz"))).left
        }
      }
      "succeedd when all variables are present" >> {
        val vars = Variables.fromMap(Map("biz" -> "name", "foo" -> "people", "baz" -> "age = 7"))
        Variables.substVars(parse("select :biz from :foo where :baz"), vars) must_===
          parse("select name from people where age = 7").right
      }
    }
  }
}
