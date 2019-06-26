/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu
package minimizers

import slamdata.Predef._

import quasar.{NonTerminal, RenderTree, RenderTreeT, Terminal}
import quasar.RenderTree.ops._

import matryoshka._

import scalaz.{Equal, Foldable, NonEmptyList}
import scalaz.std.option._
import scalaz.syntax.foldable._

private[minimizers] sealed trait Cartouche[T[_[_]]] extends Product with Serializable {
  def dropHead: Cartouche[T]
  def isEmpty: Boolean
  def length: Int
  def ::(stage: CStage[T]): Cartouche[T]
}

private[minimizers] object Cartouche {

  private[minimizers] final case class Stages[T[_[_]]](
      stages: NonEmptyList[CStage[T]]) extends Cartouche[T] {

    def dropHead: Cartouche[T] =
      Cartouche.fromFoldable(stages.tail)

    val isEmpty: Boolean = false

    def length: Int = stages.length

    def ::(stage: CStage[T]): Cartouche[T] = Cartouche.stages(stage <:: stages)
  }

  private[minimizers] final case class Source[T[_[_]]]() extends Cartouche[T] {
    val dropHead: Cartouche[T] = this
    val isEmpty: Boolean = true
    val length: Int = 0
    def ::(stage: CStage[T]): Cartouche[T] = Cartouche.stages(NonEmptyList(stage))
  }

  def source[T[_[_]]]: Cartouche[T] =
    Source[T]()

  def stages[T[_[_]]](ss: NonEmptyList[CStage[T]]): Cartouche[T] =
    Stages(ss)

  def fromFoldable[F[_]: Foldable, T[_[_]]](fa: F[CStage[T]]): Cartouche[T] =
    fa.foldMapRight1Opt(NonEmptyList(_))((s, ss) => s <:: ss)
      .fold(source[T])(stages(_))

  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]: RenderTree[Cartouche[T]] =
    RenderTree make {
      case Source() =>
        Terminal(List("Source"), None)

      case Stages(ss) =>
        NonTerminal(List("Cartouche"), None, ss.toList.map(_.render))
    }

  implicit def equal[T[_[_]]: BirecursiveT: EqualT]: Equal[Cartouche[T]] =
    Equal.equalBy[Cartouche[T], Option[NonEmptyList[CStage[T]]]] {
      case Source() => None
      case Stages(ss) => Some(ss)
    }
}
