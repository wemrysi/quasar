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

package quasar.api.services.query

import quasar.Predef._
import quasar._, fp._, fs._

import org.http4s._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.Path._, posixCodec._
import scalaz._, Scalaz._

class CompileServiceSpec extends Specification with FileSystemFixture with ScalaCheck {
  import queryFixture._

  "Compile" should {

    "plan simple query" ! prop { filesystem: SingleFileMemState =>
      // Representation of the directory as a string without the leading slash
      val pathString = printPath(filesystem.file).drop(1)
      get[String](compileService)(
        path = filesystem.parent,
        query = Some(Query(selectAll(file(filesystem.filename.value)))),
        state = filesystem.state,
        status = Status.Ok,
        response = κ(ok)
      )
    }

    "plan query with var" ! prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int) =>
      val pathString = printPath(filesystem.file).drop(1)
      val query = selectAllWithVar(file(filesystem.filename.value),varName.value)
      get[String](compileService)(
        path = filesystem.parent,
        query = Some(Query(query,varNameAndValue = Some((varName.value, var_.toString)))),
        state = filesystem.state,
        status = Status.Ok,
        response = κ(ok)
      )
    }

  }

}
