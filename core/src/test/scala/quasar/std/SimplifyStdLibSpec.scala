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

package quasar.std

import quasar.Predef._
import quasar.{Data, LogicalPlan}, LogicalPlan._

import matryoshka._
import org.specs2.execute._
// import org.specs2.matcher._
// import org.threeten.bp.{Duration, Instant, LocalDate, LocalTime}
// import scalaz._

/** Test the typers and simplifiers defined in the std lib functions themselves.
  */
class SimplifyStdLibSpec extends StdLibSpec {
  // def notHandled(prg: Fix[LogicalPlan]): Boolean = prg.unFix match {
  //   case InvokeF(???, _) => true
  //   case _ => false
  // }
  
  def run(lp: Fix[LogicalPlan], expected: Data): Result = {
    val simple = ensureCorrectTypes(lp).disjunction
    (simple must beRightDisjunction(LogicalPlan.Constant(expected))).toResult
  }
  
  def nullary(prg: Fix[LogicalPlan], expected: Data) =
    run(prg, expected)

  def unary(prg: Fix[LogicalPlan] => Fix[LogicalPlan], arg: Data, expected: Data) =
    run(prg(LogicalPlan.Constant(arg)), expected)
  
  def binary(prg: (Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan], arg1: Data, arg2: Data, expected: Data) =
    run(prg(LogicalPlan.Constant(arg1), LogicalPlan.Constant(arg2)), expected)
  
  def ternary(prg: (Fix[LogicalPlan], Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan], arg1: Data, arg2: Data, arg3: Data, expected: Data) = 
    run(prg(LogicalPlan.Constant(arg1), LogicalPlan.Constant(arg2), LogicalPlan.Constant(arg3)), expected)
}