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

package ygg.macros

import ygg.common._
import spire.macros.Syntax

object Spire {
  def cfor[A](init: A)(test: A => Boolean, next: A => A)(body: A => Unit): Unit = macro Syntax.cforMacro[A]
  def cforRange2(r1: Range, r2: Range)(body: (Int, Int) => Unit): Unit          = macro Syntax.cforRange2Macro

  // def cforRange(r: Range)(body: Int => Unit): Unit                              = macro Syntax.cforRangeMacro

  def cforRange(r: Range)(body: Int => Unit): Unit = {
    r foreach (i => body(i))
  }
}
