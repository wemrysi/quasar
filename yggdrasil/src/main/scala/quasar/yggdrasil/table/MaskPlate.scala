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

import quasar.{ParseInstruction, ParseType}
import quasar.common.{CPathField, CPathIndex, CPathMeta, CPathNode}

import tectonic.{DelegatingPlate, Plate, Signal}

private[table] final class MaskPlate[A](
    mask: ParseInstruction.Mask,
    delegate: Plate[A])
    extends DelegatingPlate(delegate)
    with CPathPlate[A]  {

  import mask._

  private val Nil = scala.Nil

  private val revIdx = masks map {
    case (path, tpes) => path.nodes.reverse -> tpes
  }

  private val foci = revIdx.keys.toSet
  private val prefixes = foci.flatMap(_.tails.toSet)

  private val vectorLoci = revIdx filter {
    case (path, tpes) =>
      tpes.contains(ParseType.Object) ||
        tpes.contains(ParseType.Array) ||
        tpes.contains(ParseType.Meta)
  }

  private var extraCursor: List[CPathNode] = Nil    // TODO this can be an Int
  private var sawSomething = false
  private var underValidVector = false

  override def nul(): Signal = {
    val tpes = revIdx.getOrElse(cursor, null)
    if (underValidVector || ((extraCursor eq Nil) && tpes != null && tpes.contains(ParseType.Null))) {
      sawSomething = true
      super.nul()
    } else {
      Signal.Continue
    }
  }

  override def fls(): Signal = {
    val tpes = revIdx.getOrElse(cursor, null)
    if (underValidVector || ((extraCursor eq Nil) && tpes != null && tpes.contains(ParseType.Boolean))) {
      sawSomething = true
      super.fls()
    } else {
      Signal.Continue
    }
  }

  override def tru(): Signal = {
    val tpes = revIdx.getOrElse(cursor, null)
    if (underValidVector || ((extraCursor eq Nil) && tpes != null && tpes.contains(ParseType.Boolean))) {
      sawSomething = true
      super.tru()
    } else {
      Signal.Continue
    }
  }

  override def map(): Signal = {
    val tpes = revIdx.getOrElse(cursor, null)
    if (underValidVector || ((extraCursor eq Nil) && tpes != null && tpes.contains(ParseType.Object))) {
      sawSomething = true
      super.map()
    } else {
      Signal.Continue
    }
  }

  override def arr(): Signal = {
    val tpes = revIdx.getOrElse(cursor, null)
    if (underValidVector || ((extraCursor eq Nil) && tpes != null && tpes.contains(ParseType.Array))) {
      sawSomething = true
      super.arr()
    } else {
      Signal.Continue
    }
  }

  override def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    val tpes = revIdx.getOrElse(cursor, null)
    if (underValidVector || ((extraCursor eq Nil) && tpes != null && tpes.contains(ParseType.Number))) {
      sawSomething = true
      super.num(s, decIdx, expIdx)
    } else {
      Signal.Continue
    }
  }

  override def str(s: CharSequence): Signal = {
    val tpes = revIdx.getOrElse(cursor, null)
    if (underValidVector || ((extraCursor eq Nil) && tpes != null && tpes.contains(ParseType.String))) {
      sawSomething = true
      super.str(s)
    } else {
      Signal.Continue
    }
  }

  override def nestMap(pathComponent: CharSequence): Signal = {
    val c = CPathField(pathComponent.toString)
    if (underValidVector) {
      super.nestMap(pathComponent)
    } else if (!(extraCursor eq Nil)) {
      extraCursor ::= c
      Signal.SkipColumn
    } else if (vectorLoci.contains(cursor) && vectorLoci(cursor).contains(ParseType.Object)) {
      underValidVector = true
      super.nestMap(pathComponent)
    } else if (!prefixes.contains(c :: cursor)) {
      extraCursor ::= c
      Signal.SkipColumn
    } else {
      super.nestMap(pathComponent)
    }
  }

  override def nestArr(): Signal = {
    val c = CPathIndex(nextIndex.head)
    if (underValidVector) {
      super.nestArr()
    } else if (!(extraCursor eq Nil)) {
      incrementIndex()
      extraCursor ::= c
      Signal.SkipColumn
    } else if (vectorLoci.contains(cursor) && vectorLoci(cursor).contains(ParseType.Array)) {
      underValidVector = true
      super.nestArr()
    } else if (!prefixes.contains(c :: cursor)) {
      incrementIndex()
      extraCursor ::= c
      Signal.SkipColumn
    } else {
      super.nestArr()
    }
  }

  override def nestMeta(pathComponent: CharSequence): Signal = {
    val c = CPathMeta(pathComponent.toString)
    if (underValidVector) {
      super.nestMeta(pathComponent)
    } else if (!(extraCursor eq Nil)) {
      extraCursor ::= c
      Signal.SkipColumn
    } else if (vectorLoci.contains(cursor) && vectorLoci(cursor).contains(ParseType.Meta)) {
      underValidVector = true
      super.nestMeta(pathComponent)
    } else if (!prefixes.contains(c :: cursor)) {
      extraCursor ::= c
      Signal.SkipColumn
    } else {
      super.nestMeta(pathComponent)
    }
  }

  override def unnest(): Signal = {
    if (!extraCursor.isEmpty) {
      extraCursor = extraCursor.tail
      Signal.Continue
    } else {
      val back = super.unnest()

      if (underValidVector && vectorLoci.contains(cursor)) {
        underValidVector = false
      }

      back
    }
  }

  override def finishRow(): Unit = {
    if (sawSomething) {
      super.finishRow()
      sawSomething = false
    }
  }
}
