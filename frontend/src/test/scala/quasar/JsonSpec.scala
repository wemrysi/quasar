/*
 * Copyright 2014–2018 SlamData Inc.
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

import scalaz.Equal

import scala.{Any, StringContext}

/**
 * Defines common machinery for testing functions that manipulate JSON
 * via differing backends.
 */
abstract class JsonSpec extends Qspec {
  // you'll get nicer error messages if you keep this simple with nice toStrings
  // like List[JValue] or something
  type JsonStream

  protected val JsonStreamEq: Equal[JsonStream]

  protected implicit def jsonStringSyntax(self: StringContext): JsonStringSyntax

  protected trait JsonStringSyntax {
    def ldjson(params: Any*): JsonStream
  }
}
