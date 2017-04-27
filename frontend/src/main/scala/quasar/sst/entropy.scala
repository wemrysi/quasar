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
import quasar.contrib.algebra._
import quasar.tpe._

import matryoshka._
import matryoshka.patterns.EnvT
import scalaz._, Scalaz._
import spire.algebra.{Field, NRoot, Trig}
import spire.math.ConvertableTo
import spire.syntax.field._
import spire.syntax.nroot._
import spire.syntax.trig._

object entropy {
  /** Algebra returning the frequency and entropy of a stat-annotated type.
    *
    * @param infEntropy    a value representing "infinite" entropy
    * @param unkCollLength the max length to use for a collection of unknown length
    */
  def apply[J: Order, A: Order: Trig: NRoot: ConvertableTo](
    infEntropy: A,
    unkCollLength: A)(
    implicit F: Field[A]
  ): Algebra[EnvT[Option[TypeStat[A]], TypeF[J, ?], ?], (A, A)] = {
    implicit val addMonoid = F.additive

    _.runEnvT match {
      case (ts, TypeF.Bottom() ) => (cnt(ts), F.zero)
      case (ts, TypeF.Top()    ) => (cnt(ts), infEntropy)
      case (ts, TypeF.Const(_) ) => (cnt(ts), F.one)
      case (ts, TypeF.Simple(s)) => (cnt(ts), (ts >>= typeStat[A] _) | simpleType(s))

      // Array entropy is calculated by treating each possible length array as a
      // bin. For each length, l, we calculate the probability of an array having
      // length l and multiply it by the cumulative entropy of the elements [0, l),
      // finally summing the individual entropies to arrive at the total entropy.
      case (ts, TypeF.Arr(-\/(xs))) =>
        val n            = cnt(ts)
        val (freqs, es0) = xs.unzip
        val probs0       = freqs map (_ / n)
        // Collapse the prefix of bins with p = 1 into a single bin
        val minSize     = probs0 indexWhere (_ < F.one) filter (_ > 1)
        val (probs, es) = minSize.fold((probs0, es0)) { l =>
                            val (comb, rest) = es0.splitAt(l)
                            (probs0 drop (l - 1), comb.suml :: rest)
                          }
        val accumEnt    = es.scanLeft(F.zero)(_ + _).drop(1)
        (n, accumEnt.fzipWith(probs)(_ * _).suml)

      // For an array where we don't know the distribution of lengths, we
      // calculate the sum of powers of the lub entropy from min to max
      // length.
      case (ts, TypeF.Arr(\/-((_, bits)))) =>
        val (min, max) =
          collectionBounds(unkCollLength, ts)

        val arrBits =
          if (bits ≟ F.one) max - min
          else (bits.fpow(max + F.one) - bits.fpow(min)) / (bits - F.one)

        (cnt(ts), arrBits)

      // We are only able to provide an upper bound as the true entropy of a
      // Map is equivalent to the entropy of a poisson binomial distribution
      // (where each bernoulli trial is whether a particular key is present
      // in the Map) which is computationally expensive when the Map is large.
      //
      // To bound the entropy, we instead calculate the entropy of a binomial
      // distribution having the same mean and number of parameters.
      case (ts, TypeF.Map(known, None)) =>
        val n              = cnt(ts)
        val (mu, totalEnt) = known foldMap (_.leftMap(_ / n))
        val p              = mu / F.fromInt(known.size)
        val `σ²`           = mu * (F.one - p)
        (n, totalEnt * gaussianEntropy(`σ²`))

      // A map with some unknowns is similar to a map where all keys are known,
      // but we instead determine the maxium sized map and use the unknown
      // key/value entropy and probability for the (max - ||known||) keys.
      case (ts, TypeF.Map(known, Some(((_, kent), (vfreq, vent))))) =>
        val n              = cnt(ts)
        val (_, max)       = collectionBounds(unkCollLength, ts)
        // The number of unknown keys in the largest possible map.
        val unkBins        = max - F.fromInt(known.size)
        val knownMuEnt     = known foldMap (_.leftMap(_ / n))
        val unknownMuEnt   = (vfreq / n, kent + vent).umap(_ * unkBins)
        val (mu, totalEnt) = knownMuEnt |+| unknownMuEnt
        val p              = mu / max
        val `σ²`           = mu * (F.one - p)
        (n, totalEnt * gaussianEntropy(`σ²`))

      case (ts, TypeF.Union(a, b, cs)) =>
        val n = cnt(ts)
        (n, (a :: b :: cs) foldMap { case (freq, ent) => ent * (freq / n) })
    }
  }

  /** Estimated entropy of the given `SimpleType`, in bits. */
  def simpleType[A](st: SimpleType)(implicit A: Field[A]): A =
    st match {
      case SimpleType.Null => A.zero
      case SimpleType.Bool => A.one
      case SimpleType.Byte => A fromInt 8
      case SimpleType.Char => A fromInt 16
      // NB: Int and Dec are currently represented via precise types that can
      //     contain more than 64 bits of information (infinite, in theory),
      //     but this serves as an estimate/representative amount relative to
      //     other types.
      case SimpleType.Int  => A fromInt 64
      case SimpleType.Dec  => A fromInt 64
    }

  /** Entropy of the given `TypeStat`, in bits, if exists. */
  def typeStat[A: Order: Field: NRoot: Trig](stat: TypeStat[A])(implicit A: ConvertableTo[A]): Option[A] = {
    val zero = Field[A].zero
    val one  = Field[A].one

    val pf: PartialFunction[TypeStat[A], A] = {
      case TypeStat.Bool(t, f) if (t + f) ≟ zero    => zero
      case TypeStat.Bool(t, f)                      =>
        val n        = t + f
        val (pt, pf) = (t / n, f / n)
        -((pt * pt.log(2)) + (pf * pf.log(2)))

      case TypeStat.Byte(_, mn, mx) if mn ≟ mx      => one
      case TypeStat.Byte(_, mn, mx)                 => ((A fromByte mx) - (A fromByte mn)).log(2)

      case TypeStat.Char(_, mn, mx) if mn ≟ mx      => one
      case TypeStat.Char(_, mn, mx)                 => ((A fromInt mx.toInt) - (A fromInt mn.toInt)).log(2)

      case TypeStat.Str(_, _, _, mn, mx) if mn ≟ mx => one
      case TypeStat.Str(_, mnl, mxl, _, _)          =>
        val charBits = A fromInt 16
        (charBits.fpow(mxl + one) - charBits.fpow(mnl)) / (charBits - one)

      // TODO: This is an upper bound, can we leverage the distribution to improve?
      case TypeStat.Int(_, mn, mx) if mn ≟ mx       => one
      case TypeStat.Int(_, mn, mx)                  => A.fromBigInt(mx - mn).log(2)

      case TypeStat.Dec(s,  _,  _)                  => s.populationVariance.cata(gaussianEntropy[A], one)
    }

    pf lift stat
  }

  ////

  private def cnt[A: Field](ots: Option[TypeStat[A]]): A =
    ots.cata(_.size, Field[A].one)

  private def collectionBounds[A](maxLen: A, ts: Option[TypeStat[A]])(implicit F: Field[A]): (A, A) =
    ts.flatMap(TypeStat.coll[A].getOption).cata({
      case (_, mn, mx) => (mn | F.zero, mx | maxLen)
    }, (F.zero, maxLen))

  /** Differential entropy of gaussian dist (in bits): 0.5log₂(2πeσ²). */
  private def gaussianEntropy[A](`σ²`: A)(implicit A: Field[A], T: Trig[A]): A = {
    val two = A fromInt 2
    A.fromDouble(0.5) * (two * T.pi * T.e * `σ²`).log / two.log
  }
}
