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

package quasar.frontend.logicalplan

import slamdata.Predef._
import quasar.Type
import quasar.fp.ski.κ

import scala.Symbol

import matryoshka.data.Fix
import org.specs2.execute._
import scalaz.Cofree

class InferTypesSpec extends quasar.Qspec {

  val lpr = new LogicalPlanR[Fix[LogicalPlan]]

  def infer(lp: Fix[LogicalPlan])(eval: Cofree[LogicalPlan, Type] => ResultLike): Result = 
    lpr.inferTypes(Type.Top, lp).fold(κ(failure), eval(_).toResult)
  
  "inferTypes" should  {

    "be Top for Free" in {
      val lp: Fix[LogicalPlan] = mkFree("sym")
      infer(lp) { cf =>
        cf.head must_== Type.Top
        cf.tail must_== mkFree("sym").unFix
      }
    }

    "be Top for Let(Free, Free)" in {
      val lp: Fix[LogicalPlan] = Fix(Let(Symbol("sym"), mkFree("sym"), mkFree("sym")))
      infer(lp) { cf =>
        cf.head must_== Type.Top
        val (sym, form, in) = let.getOption(cf.tail).get
        sym must_== Symbol("sym")
        form.head must_== Type.Top
        free.getOption(form.tail).get must_== Symbol("sym")
        free.getOption(in.tail).get must_== Symbol("sym")
      }
    }
  }

  private def mkFree(symbol: String): Fix[LogicalPlan] =
    Fix(Free(Symbol(symbol)))

}
