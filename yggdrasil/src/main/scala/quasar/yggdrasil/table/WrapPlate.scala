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

import quasar.ParseInstruction
import quasar.common.CPathField

import tectonic.{DelegatingPlate, Plate, Signal}

private[table] final class WrapPlate[A](
    wrap: ParseInstruction.Wrap,
    delegate: Plate[A])
    extends DelegatingPlate(delegate)
    with CPathPlate[A]  {

  import wrap._

  private val rfocus = path.nodes.reverse
  private val rfocusPlusWrap = CPathField(name) :: rfocus

  override def nul(): Signal = {
    nestFurther()
    super.nul()
  }

  override def fls(): Signal = {
    nestFurther()
    super.fls()
  }

  override def tru(): Signal = {
    nestFurther()
    super.tru()
  }

  override def map(): Signal = {
    nestFurther()
    super.map()
  }

  override def arr(): Signal = {
    nestFurther()
    super.arr()
  }

  override def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    nestFurther()
    super.num(s, decIdx, expIdx)
  }

  override def str(s: CharSequence): Signal = {
    nestFurther()
    super.str(s)
  }

  override def nestMap(pathComponent: CharSequence): Signal = {
    nestFurther()
    super.nestMap(pathComponent)
  }

  override def nestArr(): Signal = {
    nestFurther()
    super.nestArr()
  }

  override def nestMeta(pathComponent: CharSequence): Signal = {
    nestFurther()
    super.nestMeta(pathComponent)
  }

  override def unnest(): Signal = {
    unnestFurther()
    super.unnest()
  }

  override def finishRow(): Unit = {
    unnestFurther()
    super.finishRow()
  }

  private def nestFurther(): Unit = {
    if (atFocus()) {
      super.nestMap(name)
    }
  }

  private def unnestFurther(): Unit = {
    if (atFocusPlusWrap()) {
      super.unnest()
    }
  }

  private def atFocus() = cursor == rfocus
  private def atFocusPlusWrap() = cursor == rfocusPlusWrap
}
