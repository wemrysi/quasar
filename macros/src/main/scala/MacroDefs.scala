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

import quasar.Predef._
import scala.{ Any, StringContext }
import jawn.Facade

class JsonStringContexts[A] {
  implicit class Interpolator(sc: StringContext)(implicit val facade: Facade[A]) {
    def json(args: Any*): A            = macro JsonMacroImpls.singleImpl[A]
    def jsonSeq(args: Any*): Vector[A] = macro JsonMacroImpls.manyImpl[A]
  }
}

object JsonMacros {
  object EJson extends JsonStringContexts[quasar.ejson.EJson[quasar.Data]] {
    implicit def ejsonFacade = quasar.Data.EJsonDataFacade
  }
  object Argonaut extends JsonStringContexts[argonaut.Json] {
    implicit def argonautFacade = argonaut.JawnParser.facade
  }
  object Jawn extends JsonStringContexts[jawn.ast.JValue] {
    implicit def janwFacade = jawn.ast.JawnFacade
  }
}
