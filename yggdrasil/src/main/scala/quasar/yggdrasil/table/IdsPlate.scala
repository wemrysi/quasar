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

import cats.effect.Sync

import tectonic.{Plate, Signal}

private[table] final class IdsPlate[A] private (delegate: Plate[A]) extends Plate[A] {
  private var sawSomething = false
  private var id = 0L   // 450 exabytes is enough for anyone

  def nul(): Signal = {
    emitId()
    delegate.nul()
  }

  def fls(): Signal = {
    emitId()
    delegate.fls()
  }

  def tru(): Signal = {
    emitId()
    delegate.tru()
  }

  def map(): Signal = {
    emitId()
    delegate.map()
  }

  def arr(): Signal = {
    emitId()
    delegate.arr()
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    emitId()
    delegate.num(s, decIdx, expIdx)
  }

  def str(s: CharSequence): Signal = {
    emitId()
    delegate.str(s)
  }

  def nestMap(pathComponent: CharSequence): Signal = {
    emitId()
    delegate.nestMap(pathComponent)
  }

  def nestArr(): Signal = {
    emitId()
    delegate.nestArr()
  }

  def nestMeta(pathComponent: CharSequence): Signal = {
    emitId()
    delegate.nestMeta(pathComponent)
  }

  def unnest(): Signal = {
    emitId()
    delegate.unnest()
  }

  def finishRow(): Unit = {
    if (sawSomething) {
      delegate.unnest()
      sawSomething = false
    }

    delegate.finishRow()
  }

  def finishBatch(terminal: Boolean): A =
    delegate.finishBatch(terminal)

  private def emitId(): Unit = {
    if (!sawSomething) {
      sawSomething = true

      delegate.nestArr()
      delegate.num(id.toString, -1, -1)
      delegate.unnest()
      delegate.nestArr()

      id += 1L
    }
  }
}

private[table] object IdsPlate {
  def apply[F[_]: Sync, A](delegate: Plate[A]): F[Plate[A]] =
    Sync[F].delay(new IdsPlate(delegate))
}
