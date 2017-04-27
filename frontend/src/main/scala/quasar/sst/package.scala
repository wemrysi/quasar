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

package quasar

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson.{BinaryTag, EJson, CommonEJson => C, ExtEJson => E, Meta, Null, SizedTypeTag, Str}
import quasar.fp.ski.κ
import quasar.tpe._

import matryoshka._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import spire.algebra.Field
import spire.math.ConvertableTo

package object sst {
  /** Statistical Structural Type */
  type SSTF[J, A, B] = EnvT[Option[TypeStat[A]], TypeF[J, ?], B]
  type SST[J, A]     = StructuralType[J, Option[TypeStat[A]]]

  object SST {
    def fromData[J: Order, A: ConvertableTo: Field: Order](
      count: A,
      data: Data
    )(implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson]
    ): SST[J, A] = {
      val ejs = data.hylo[CoEnv[Data, EJson, ?], J](
        interpret(κ(C(Null[J]()).embed), elideNonBinaryMetadata[J] >>> (_.embed)),
        Data.toEJson[EJson])
      StructuralType.fromEJson[J](TypeStat.fromTypeFƒ(count), ejs)
    }

    def fromEJson[J: Order, A: ConvertableTo: Field: Order](
      count: A,
      ejson: J
    )(implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson]
    ): SST[J, A] =
      StructuralType.fromEJson[J](
        TypeStat.fromTypeFƒ(count),
        ejson.transCata[J](elideNonBinaryMetadata[J]))

    ////

    private def elideNonBinaryMetadata[J](
      implicit J: Recursive.Aux[J, EJson]
    ): EJson[J] => EJson[J] = {
      case ejs @ EncodedBinary(_) => ejs
      case other                  => EJson.elideMetadata[J] apply other
    }
  }

  object EncodedBinary {
    def unapply[J](ejs: EJson[J])(implicit J: Recursive.Aux[J, EJson]): Option[BigInt] =
      ejs match {
        case E(Meta(Embed(SizedTypeTag(BinaryTag, size)), Embed(C(Str(_))))) => some(size)
        case _                                                               => none
      }
  }
}
