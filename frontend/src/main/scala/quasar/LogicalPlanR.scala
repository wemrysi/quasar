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

import quasar.contrib.pathy._
import quasar.Predef._
import quasar.{LogicalPlan => LP, _}

import matryoshka._
import shapeless.Nat

class LogicalPlanR[T[_[_]]: Corecursive] {
  def let(let: Symbol, form: T[LP], in: T[LP]): T[LP] =
    LP.Let(let, form, in).embed

  def constant(data: Data): T[LP] =
    LP.Constant[T[LP]](data).embed

  def typecheck(expr: T[LP], typ: Type, cont: T[LP], fallback: T[LP]): T[LP] =
    LP.Typecheck(expr, typ, cont, fallback).embed

  def free(name: Symbol): T[LP] =
    LP.Free[T[LP]](name).embed

  def read(path: FPath): T[LP] =
    LP.Read[T[LP]](path).embed

  def invoke[N <: Nat](func: GenericFunc[N], values: Func.Input[T[LP], N]): T[LP] =
    LP.Invoke(func, values).embed
}
