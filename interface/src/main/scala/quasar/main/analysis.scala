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
import quasar.contrib.pathy._
import quasar.ejson.EJson
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}
import quasar.sst._
import quasar.std.StdLib

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.stream._
import spire.algebra.Field
import spire.math.ConvertableTo

object analysis {
  /** Knobs controlling various aspects of SST compression.
    *
    * @param arrayMaxSize    arrays larger than this will be compressed
    * @param mapMaxSize      maps larger than this will be compressed
    * @param stringMaxLength all strings longer than this are compressed to char[]
    * @param unionMaxSize    unions larger than this will be compressed
    */
  final case class CompressionSettings(
    arrayMaxLength:  Positive,
    mapMaxSize:      Positive,
    stringMaxLength: Positive,
    unionMaxSize:    Positive
  )

  object CompressionSettings {
    val DefaultArrayMaxLength:  Positive = 64L
    val DefaultMapMaxSize:      Positive = 64L
    val DefaultStringMaxLength: Positive = 64L
    val DefaultUnionMaxSize:    Positive =  1L

    val Default: CompressionSettings =
      CompressionSettings(
        arrayMaxLength  = DefaultArrayMaxLength,
        mapMaxSize      = DefaultMapMaxSize,
        stringMaxLength = DefaultStringMaxLength,
        unionMaxSize    = DefaultUnionMaxSize)
  }

  /** Reduces the input to an `SST` summarizing its structure. */
  def extractSchema[J: Order, A: ConvertableTo: Field: Order](
    settings: CompressionSettings
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): Process1[Data, SST[J, A]] = {
    val structuralTrans =
      compression.coalesceWithUnknown[J, A]                    >>>
      compression.coalesceKeys[J, A](settings.mapMaxSize)      >>>
      compression.z85EncodedBinary[J, A]                       >>>
      compression.limitStrings[J, A](settings.stringMaxLength) >>>
      compression.limitArrays[J, A](settings.arrayMaxLength)   >>>
      compression.coalescePrimary[J, A]                        >>>
      compression.narrowUnion[J, A](settings.unionMaxSize)

    val compress = (sst: SST[J, A]) => {
      val compSST = sst.transCata[SST[J, A]](structuralTrans)
      (sst ≠ compSST) option compSST
    }

    process1.lift(SST.fromData[J, A](Field[A].one, _: Data))
      .reduce((x, y) => repeatedly(compress)(x |+| y))
  }

  /** A random sample of the dataset at the given path. */
  def sample[S[_]](file: AFile, size: Positive)(
    implicit Q: QueryFile.Ops[S]
  ): Process[Q.M, Data] =
    Q.evaluate(sampleQuery(file, size)).translate(Q.transforms.dropPhases)

  /** Query representing a random sample of `size` items from the specified file. */
  def sampleQuery(file: AFile, size: Positive): Fix[LogicalPlan] = {
    val lpr   = new LogicalPlanR[Fix[LogicalPlan]]
    val dsize = Data._int(size.value)
    lpr.invoke2(StdLib.set.Sample, lpr.read(file), lpr.constant(dsize))
  }

  /** An eager random sample of the dataset at the given path. */
  def sampleResults[S[_]](file: AFile, size: Positive)(
    implicit Q: QueryFile.Ops[S]
  ): Q.M[Process0[Data]] =
    Q.transforms.dropPhases(Q.results(sampleQuery(file, size)))
}
