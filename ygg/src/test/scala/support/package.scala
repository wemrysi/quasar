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

package ygg

import ygg.macros._
import common._, json._

package object tests extends ygg.tests.pkg.TestsPackage {
  implicit final class JvalueInterpolator(sc: StringContext) {
    def json(args: Any*): JValue             = macro JValueMacros.jsonInterpolatorImpl
    def jsonMany(args: Any*): Vector[JValue] = macro JValueMacros.jsonManyInterpolatorImpl
  }
}
