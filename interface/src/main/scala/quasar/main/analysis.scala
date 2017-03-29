/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.main

import quasar.Data
import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.fp.numeric.Positive
import quasar.sst._

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.stream._
import spire.algebra.Field
import spire.math.ConvertableTo

object analysis {
  /** Reduces the input to an `SST` describing the `Data` seen so far. */
  def extractSchema[J: Order, A: ConvertableTo: Field: Order](
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): Process1[Data, SST[J, A]] = {
    // TODO: CompressionSettings
    val stringLimit: Positive = 128L
    val minimumObs : Positive =   1L
    val distRatio             = 0.80

    val preprocess =
      compression.z85EncodedBinary[J, A] >>> compression.limitStrings[J, A](stringLimit)

    val compressTrans =
      compression.coalesceKeys[J, A](minimumObs, distRatio)   >>>
      preprocess                                              >>>
      compression.coalescePrimary[J, A](minimumObs, distRatio)

    val compress = (sst: SST[J, A]) => {
      val compSST = sst.transCata[SST[J, A]](compressTrans)
      (sst ≠ compSST) option compSST
    }

    process1.lift(SST.fromData[J, A](Field[A].one, _: Data))
      .map(_ transCata[SST[J, A]] preprocess)
      .reduceMonoid
      .map(repeatedly(compress))
  }
}
