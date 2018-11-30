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

package quasar.yggdrasil.table

import slamdata.Predef.{inline, Nil}

import cats.effect.Sync

import quasar.common.{CPath, CPathNode}

import tectonic.{Plate, Signal}

private[table] final class ProjectPlate[A] private (
    path: CPath,
    delegate: Plate[A])
    extends Plate[A]
    with ContinuingNestPlate[A]
    with CPathPlate[A] {

  private val SEARCHING: Int = 0
  private val DESCENDING: Int = 1
  private val FOCUSED: Int = 2
  private val FINISHED_UNFOCUSED: Int = 3
  private val FINISHED_FOCUSED: Int = 4

  private var focus: List[CPathNode] = path.nodes
  private var focusDepth: Int = 0
  private var projectState: Int = if (focus eq Nil) FOCUSED else SEARCHING

  def nul(): Signal =
    if (focused())
      delegate.nul()
    else
      Signal.Continue

  def fls(): Signal =
    if (focused())
      delegate.fls()
    else
      Signal.Continue

  def tru(): Signal =
    if (focused())
      delegate.tru()
    else
      Signal.Continue

  def map(): Signal =
    if (focused())
      delegate.map()
    else
      Signal.Continue

  def arr(): Signal =
    if (focused())
      delegate.arr()
    else
      Signal.Continue

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal =
    if (focused())
      delegate.num(s, decIdx, expIdx)
    else
      Signal.Continue

  def str(s: CharSequence): Signal =
    if (focused())
      delegate.str(s)
    else
      Signal.Continue

  override def nestMap(pathComponent: CharSequence): Signal = {
    super.nestMap(pathComponent)

    projectState match {
      case FOCUSED =>
        focusDepth += 1
        delegate.nestMap(pathComponent)

      case SEARCHING =>
        search(true)

      case DESCENDING =>
        search(false)

      case _ =>
        Signal.SkipRow
    }
  }

  override def nestArr(): Signal = {
    super.nestArr()

    projectState match {
      case FOCUSED =>
        focusDepth += 1
        delegate.nestArr()

      case SEARCHING =>
        search(true)

      case DESCENDING =>
        search(false)

      case _ =>
        Signal.SkipRow
    }
  }

  override def nestMeta(pathComponent: CharSequence): Signal = {
    super.nestMeta(pathComponent)

    projectState match {
      case FOCUSED =>
        focusDepth += 1
        delegate.nestMeta(pathComponent)

      case SEARCHING =>
        search(true)

      case DESCENDING =>
        search(false)

      case _ =>
        Signal.SkipRow
    }
  }

  override def unnest(): Signal = {
    super.unnest()

    val signal = if (projectState > FOCUSED) {
      Signal.SkipRow
    } else if (focusDepth == 0) {
      projectState = if (focused()) FINISHED_FOCUSED else FINISHED_UNFOCUSED
      Signal.SkipRow
    } else if (focused()) {
      delegate.unnest()
    } else {
      Signal.Continue
    }

    focusDepth -= 1

    signal
  }

  override def finishRow(): Unit = {
    super.finishRow()

    if (focused() || projectState == FINISHED_FOCUSED) {
      delegate.finishRow()
    }

    if (path.nodes ne Nil) {
      focus = path.nodes
      focusDepth = 0
      projectState = SEARCHING
    }
  }

  def finishBatch(terminal: Boolean): A =
    delegate.finishBatch(terminal)

  @inline
  private final def focused(): Boolean =
    projectState == FOCUSED

  private final def search(initial: Boolean): Signal =
    if ((!initial || (cursor.tail eq Nil)) && cursor.head == focus.head) {
      focus = focus.tail
      focusDepth = 0
      if (focus eq Nil) {
        projectState = FOCUSED
      } else if (initial) {
        projectState = DESCENDING
      }
      Signal.Continue
    } else {
      focusDepth += 1
      Signal.SkipColumn
    }
}

private[table] object ProjectPlate {

  def apply[F[_]: Sync, A](
      path: CPath,
      delegate: Plate[A])
      : F[Plate[A]] =
    Sync[F].delay(new ProjectPlate(path, delegate))
}
