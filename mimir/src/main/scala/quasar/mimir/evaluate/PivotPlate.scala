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

package quasar.mimir.evaluate

import quasar.{IdStatus, ParseInstruction}
import quasar.common.{CPathField, CPathIndex, CPathMeta, CPathNode}
import quasar.yggdrasil.table.CPathPlate

import tectonic.{DelegatingPlate, Plate, Signal}

// currently assumes retain = false, meaning you *cannot* have any non-shifted stuff in the row
final class PivotPlate[A](
    pivot: ParseInstruction.Pivot,
    delegate: Plate[A])
    extends DelegatingPlate(delegate)
    with CPathPlate[A] {

  import pivot._

  private val rfocus = path.nodes.reverse

  // if this is true, it means we're under the focus and might unnest once without moving
  private var underFocus = false
  private var suppress = false

  private val IdOnly = IdStatus.IdOnly
  private val IncludeId = IdStatus.IncludeId
  // private val ExcludeId = IdStatus.ExcludeId

  override def nul(): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.nul()
  }

  override def fls(): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.fls()
  }

  override def tru(): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.tru()
  }

  override def map(): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.map()
  }

  override def arr(): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.arr()
  }

  override def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.num(s, decIdx, expIdx)
  }

  override def str(s: CharSequence): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.str(s)
  }

  // TODO suppress nestMap?
  override def nestMap(pathComponent: CharSequence): Signal = {
    if (atFocus() && !underFocus) {
      val back = if (idStatus eq IdOnly) {
        delegate.str(pathComponent)
        suppress = true
        Signal.SkipColumn
      } else if (idStatus eq IncludeId) {
        delegate.nestArr()
        delegate.str(pathComponent)
        delegate.unnest()
        delegate.nestArr()
        Signal.Continue
      } else {
        Signal.Continue
      }

      underFocus = true

      back
    } else {
      super.nestMap(pathComponent)
    }
  }

  override def nestArr(): Signal = {
    if (atFocus() && !underFocus) {
      val back = if (idStatus eq IdOnly) {
        delegate.num(nextIndex.head.toString, -1, -1)
        suppress = true
        Signal.SkipColumn
      } else if (idStatus eq IncludeId) {
        delegate.nestArr()
        delegate.num(nextIndex.head.toString, -1, -1)
        delegate.unnest()
        delegate.nestArr()
        Signal.Continue
      } else {
        Signal.Continue
      }

      underFocus = true

      back
    } else {
      super.nestArr()
    }
  }

  override def nestMeta(pathComponent: CharSequence): Signal = {
    if (atFocus() && !underFocus) {
      val back = if (idStatus eq IdOnly) {
        delegate.str(pathComponent)
        suppress = true
        Signal.SkipColumn
      } else if (idStatus eq IncludeId) {
        delegate.nestArr()
        delegate.str(pathComponent)
        delegate.unnest()
        delegate.nestArr()
        Signal.Continue
      } else {
        Signal.Continue
      }

      underFocus = true

      back
    } else {
      super.nestMeta(pathComponent)
    }
  }

  override def unnest(): Signal = {
    if (atFocus() && underFocus) {
      if (idStatus eq IncludeId) {
        delegate.unnest()   // get out of the second array component
      }

      // we just unnested once at the focus
      def renest(cursor: List[CPathNode]): Unit = cursor match {
        case hd :: tail =>
          delegate.unnest()
          renest(tail)

          hd match {
            case CPathField(field) => delegate.nestMap(field)
            case CPathIndex(_) => delegate.nestArr()
            case CPathMeta(field) => delegate.nestMeta(field)
            case c => sys.error(s"impossible component $c")
          }

          ()

        case Nil =>
      }

      delegate.finishRow()
      renest(cursor)
      underFocus = false
      suppress = false

      Signal.Continue
    } else {
      super.unnest()
    }
  }

  private def atFocus(): Boolean = cursor == rfocus
}
