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

package quasar.impl.evaluate

import slamdata.Predef._
import quasar.ParseInstructionSpec
import quasar.common.data.RValue

import cats.effect.IO
import cats.effect.concurrent.Ref

import jawn.{AsyncParser, Facade}

import qdata.json.QDataFacade

import scalaz.std.list._
import scalaz.syntax.traverse._

import shims._

object RValueParseInstructionSpec extends ParseInstructionSpec {
  type JsonStream = List[RValue]

  def evalIds(stream: JsonStream): JsonStream =
    Ref[IO].of(0L)
      .flatMap(r => stream.traverse(RValueParseInstructionInterpreter.interpretIds(r, _)))
      .unsafeRunSync()

  def evalMask(mask: Mask, stream: JsonStream): JsonStream =
    stream.flatMap(RValueParseInstructionInterpreter.interpretMask(mask, _).toList)

  def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream =
    stream.flatMap(RValueParseInstructionInterpreter.interpretPivot(pivot, _))

  def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream =
    stream.map(RValueParseInstructionInterpreter.interpretWrap(wrap, _))

  def evalProject(project: Project, stream: JsonStream): JsonStream =
    stream.flatMap(RValueParseInstructionInterpreter.interpretProject(project, _))

  protected def ldjson(str: String): JsonStream = {
    implicit val facade: Facade[RValue] = QDataFacade[RValue](isPrecise = false)

    val parser: AsyncParser[RValue] =
      AsyncParser[RValue](AsyncParser.ValueStream)

    val events1: List[RValue] = parser.absorb(str).right.get.toList
    val events2: List[RValue] = parser.finish().right.get.toList

    events1 ::: events2
  }
}
