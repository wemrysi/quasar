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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, CommonEJson => C, Str}
import quasar.fp.numeric._
import quasar.fp.ski.ι
import quasar.tpe._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.patterns.EnvT
import matryoshka.implicits._
import scalaz._, Scalaz._
import spire.algebra.{Field, Ring}
import spire.math.ConvertableTo
import spire.syntax.field._

object compression {
  import TypeF._
  private val TS = TypeStat

  /** Compress a map by moving the largest group of keys having the same primary
    * type to the unknown key field and their values to the unknown value field.
    *
    * Only maps having at least `minObs` observations and where the ratio
    * of the size of the map to the number of
    * observations >= `distinctRatio` are considered.
    */
  def coalesceKeys[J: Order, A: Order: Field: ConvertableTo](
    minObs: Positive,
    keyRatio: Double
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = {
    type T = Fix[TypeF[J, ?]]
    type M = PrimaryType ==>> (J ==>> Unit)

    totally {
      case sstf @ EnvT((ts, Map(kn, unk))) if sufficientlyDiverse(minObs, keyRatio, ts, kn) =>
        val grouped = kn.foldlWithKey(IMap.empty: M) { (m, j, _) =>
          primaryTypeOf(j.project).fold(m) { pt =>
            m.alter(pt, _ map (_ insert (j, ())) orElse some(IMap.singleton(j, ())))
          }
        }

        val (kn1, unk1) = grouped.maximumBy(_.size).fold((kn, unk)) { m =>
          val toCompress = kn intersection m

          val compressed = toCompress.toList foldMap { case (j, sst) =>
            (primarySST(size1(sst.copoint), j), sst)
          }

          (kn \\ toCompress, some(compressed) |+| unk)
        }

        envT(ts, map[J, SST[J, A]](kn1, unk1))
    }
  }

  /** Compress the largest group of values in a union having the same
    * primary type to that type.
    *
    * Only unions having at least `minObs` observations and where the ratio
    * of the size of the union to the number of
    * observations >= `distinctRatio` are considered.
    */
  def coalescePrimary[J: Order, A: Order: Field: ConvertableTo](
    minObs: Positive,
    distinctRatio: Double
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = {
    type T = Fix[TypeF[J, ?]]
    type M = PrimaryType ==>> NonEmptyList[SST[J, A]]

    totally {
      case sstf @ EnvT((ts, Unioned(xs))) if sufficientlyDiverse(minObs, distinctRatio, ts, xs) =>
        val grouped   = xs.list.groupBy(sst => TypeF.primary[J](sst.project.lower))
        val primaries = grouped.toList.map(_.bitraverse(ι, some)).unite

        val compressed = primaries.maximumBy(_._2.length).fold(grouped) {
          case (pt, ssts) =>
            val reduced = ssts.foldMap(sst => sst.project match {
              case EnvT((x, Const(j))) => sstMeasure[J, A].set(x)(primarySST(size1(x), j))
              case _                   => sst
            })
            grouped.insert(some(pt), reduced.wrapNel)
        }

        compressed.foldMap(_.list) match {
          case ICons(x, ICons(y, zs)) => envT(ts, union[J, SST[J, A]](x, y, zs))
          case ICons(x, INil())       => envT(ts, x.project.lower)
          case INil()                 => sstf
        }
    }
  }

  /** Replace arrays of known size longer than the given limit with an array of unknown size. */
  def limitArrays[J: Order, A: Order](maxLength: Positive)(
    implicit
    A : Field[A],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case EnvT((ts, Arr(-\/(elts)))) if elts.length > maxLength.value =>
      val (cnt, len) = (size1(ts), A fromInt elts.length)
      envT(some(TS.coll(cnt, some(len), some(len))), arr(\/-(elts.suml)))
  }

  /** Replace literal string types longer than the given limit with `char[]`. */
  def limitStrings[J, A](maxLength: Positive)(
    implicit
    A : Ring[A],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case EnvT((ts, Const(Embed(C(Str(s)))))) if s.length > maxLength.value =>
      val (cnt, len) = (size1(ts), A fromInt s.length)
      envT(some(TS.coll(cnt, some(len), some(len))), charArr(cnt))
  }

  /** Replace encoded binary strings with `byte[]`. */
  def z85EncodedBinary[J, A](
    implicit
    A : Field[A],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case EnvT((ts, Const(Embed(EncodedBinary(size))))) =>
      // NB: Z85 uses 5 chars for every 4 bytes.
      val (cnt, len) = (size1(ts), A.fromBigInt(size) * A.fromInt(4) / A.fromInt(5))
      envT(some(TS.coll(cnt, some(len), some(len))), byteArr(cnt))
  }

  ////

  private def sstMeasure[J, A] = StructuralType.measure[J, Option[TypeStat[A]]]

  private def byteArr[J, A](cnt: A): TypeF[J, SST[J, A]] =
    simpleArr(cnt, SimpleType.Byte)

  private def charArr[J, A](cnt: A): TypeF[J, SST[J, A]] =
    simpleArr(cnt, SimpleType.Char)

  private def simpleArr[J, A](cnt: A, st: SimpleType): TypeF[J, SST[J, A]] =
    arr[J, SST[J, A]](envT(some(TS.count(cnt)), simple[J, SST[J, A]](st)).embed.right)

  /** Returns the SST of the primary type of the given EJson value.
    *
    * ∀ a j. Const(j).embed <: primarySST(a, j).toType[T]
    */
  private def primarySST[J: Order, A: ConvertableTo: Field: Order](cnt: A, j: J)(
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SST[J, A] = j.project match {
    case ej @ SimpleEJson(s) => envT(some(TS.fromEJson(cnt, ej)), simple[J, SST[J, A]](s)).embed
    case ej @ C(Str(_))      => envT(some(TS.fromEJson(cnt, ej)), charArr[J, A](cnt)).embed
    case _                   => SST.fromEJson(cnt, j)
  }

  private def size1[A](ots: Option[TypeStat[A]])(implicit R: Ring[A]): A =
    ots.cata(_.size, R.one)

  /** Returns whether the given foldable is "sufficiently diverse" based on the
    * provided parameters. Diversity is defined as the ratio of the size of F
    * to the number of observations.
    */
  private def sufficientlyDiverse[F[_]: Foldable, A: Order, B](
    minObs: Positive,
    ratio: Double,
    ts: Option[TypeStat[A]],
    fb: F[B]
  )(implicit A: Field[A]): Boolean = {
    val (cnt, len) = (size1(ts), fb.length)
    val alen       = A fromInt len
    len >= minObs.value && (alen / cnt) >= A.fromDouble(ratio)
  }
}
