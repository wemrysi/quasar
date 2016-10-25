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

package quasar.frontend

import quasar.Predef._
import quasar.{LogicalPlan => LP, _}

import matryoshka._

trait LogicalPlanHelpers {
  def fixLet(let: Symbol, form: Fix[LP], in: Fix[LP]): Fix[LP] =
    Fix[LP](LP.Let(let, form, in))

  def fixConstant(data: Data): Fix[LP] =
    Fix[LP](LP.Constant(data))

  def fixTypecheck(expr: Fix[LP], typ: Type, cont: Fix[LP], fallback: Fix[LP]): Fix[LP] =
    Fix[LP](LP.Typecheck(expr, typ, cont, fallback))
}
