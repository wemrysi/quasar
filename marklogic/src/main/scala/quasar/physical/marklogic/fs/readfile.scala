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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar._
import quasar.fs._
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.physical.marklogic._

import com.marklogic.xcc.ResultItem
import com.marklogic.xcc.types._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

object readfile {

  def toData(i: ResultItem): Vector[Data] = i.getItem match {
    case a: ArrayNode   => ???
    case a: BooleanNode => ???
    case a: CtsBox      => ???
    case a: CtsCircle   => ???
    case a: CtsPoint    => ???
    case a: CtsPolygon  => ???
    case a: JSArray     => ???
    case a: JSObject    => ???
    //case a: JsonItem    => ???
    case a: NullNode    => ???
    case a: NumberNode  => ???
    case a: ObjectNode  => ???
    case a: ResultItem  => ???
    //case a: XdmAtomic   => ???
    case a: XdmAttribute => ???
    case a: XdmBinary    => ???
    case a: XdmComment   => ???
    case a: XdmDocument  => ???
    case a: XdmElement   => println(a.asW3cElement); ???
    //case a: XdmNode      => ???
    case a: XdmProcessingInstruction => ???
    case a: XdmText => ???
    case a: XSAnyURI => ???
    case a: XSBase64Binary => ???
    case a: XSBoolean => ???
    case a: XSDate => ???
    case a: XSDateTime => ???
    case a: XSDayTimeDuration => ???
    case a: XSDecimal => ???
    case a: XSDouble => ???
    //case a: XSDuration => ???
    case a: XSFloat => ???
    case a: XSGDay => ???
    case a: XSGMonth => ???
    case a: XSGMonthDay => ???
    case a: XSGYear => ???
    case a: XSGYearMonth => ???
    case a: XSHexBinary => ???
    case a: XSInteger => ???
    case a: XSQName => ???
    case a: XSString => ???
    case a: XSTime => ???
    case a: XSUntypedAtomic => ???
    case a: XSYearMonthDuration          => ???
  }

  def interpret[S[_]](implicit
    S0:           Task :<: S,
    S1:           Read[Client, ?] :<: S,
    state:        KeyValueStore.Ops[ReadFile.ReadHandle, Process[Task, Vector[Data]], S],
    seq:          MonotonicSeq.Ops[S]
  ): ReadFile ~> Free[S,?] =
    quasar.fs.impl.readFromProcess { (file, readOpts) =>
      val dirPath = fileParent(file) </> dir(fileName(file).value)
      Client.exists(dirPath).ifM(
        Client.readDirectory(dirPath).map(_.map(toData).right[FileSystemError]),
        pathErr(pathNotFound(file)).left[Process[Task, Vector[Data]]].pure[Free[S,?]])
    }

}
