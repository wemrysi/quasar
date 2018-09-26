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

package quasar.impl

import slamdata.Predef._
import quasar.common.data.Data
import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.contrib.iota.copkTraverse
import quasar.sst._

import scala.concurrent.ExecutionContext

import cats.effect.Effect
import fs2.{Chunk, Pipe}
import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import spire.algebra.Field
import spire.math.ConvertableTo

package object schema {

  /** Reduces the input to an `SST` summarizing its structure. */
  def extractSst[F[_], J: Order, A: ConvertableTo: Field: Order](
      config: SstConfig[J, A])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Pipe[F, Chunk[Data], SST[J, A]] =
    extractSst0[F, J, A](config)(f => _.map(f))

  /** Reduces the input to an `SST` summarizing its structure. */
  def extractSstAsync[F[_]: Effect, J: Order, A: ConvertableTo: Field: Order](
      config: SstConfig[J, A],
      paralellism: Int)(
      implicit
      ec: ExecutionContext,
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Pipe[F, Chunk[Data], SST[J, A]] =
    extractSst0[F, J, A](config)(f => _.mapAsyncUnordered(paralellism)(c => Effect[F].delay(f(c))))

  ////

  private def extractSst0[F[_], J: Order, A: ConvertableTo: Field: Order](
      config: SstConfig[J, A])(
      f: (Chunk[Data] => Option[SST[J, A]]) => Pipe[F, Chunk[Data], Option[SST[J, A]]])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Pipe[F, Chunk[Data], SST[J, A]] = {

    val thresholding: ElgotCoalgebra[SST[J, A] \/ ?, SSTF[J, A, ?], SST[J, A]] = {
      val independent =
        orOriginal(applyTransforms(
          compression.limitStrings[J, A](config.stringMaxLength, config.stringPreserveStructure)))

      compression.limitArrays[J, A](config.arrayMaxLength, config.retainIndicesSize)
        .andThen(_.bimap(_.transAna[SST[J, A]](independent), independent))
    }

    val reduction: SSTF[J, A, SST[J, A]] => Option[SSTF[J, A, SST[J, A]]] =
      applyTransforms(
        compression.coalesceWithUnknown[J, A](config.retainKeysSize),
        compression.coalesceKeys[J, A](config.mapMaxSize, config.retainKeysSize),
        compression.coalescePrimary[J, A](config.stringPreserveStructure),
        compression.narrowUnion[J, A](config.unionMaxSize, config.stringPreserveStructure))

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    def iterate(sst: SST[J, A]): Option[SST[J, A]] = {
      var changed = false

      val compressed = sst.transAna[SST[J, A]](sstf =>
        reduction(sstf).fold(sstf) { next =>
          changed = true
          next
        })

      changed option compressed
    }

    def fromData(d: Data): SST[J, A] =
      SST.fromData[J, A](Field[A].one, d).elgotApo[SST[J, A]](thresholding)

    @SuppressWarnings(Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Var",
      "org.wartremover.warts.While"))
    def reduceChunk(c: Chunk[Data]): Option[SST[J, A]] =
      if (c.isEmpty) none
      else if (c.size == 1) some(fromData(c(0)))
      else {
        var i = 1
        var acc = fromData(c(0))
        while (i < c.size) {
          acc = repeatedly(iterate)(acc |+| fromData(c(i)))
          i = i + 1
        }
        some(acc)
      }

    _.through(f(reduceChunk)).unNone.scan1((x, y) => repeatedly(iterate)(x |+| y))
  }
}
