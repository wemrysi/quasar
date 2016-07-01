/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.qscript

import quasar.ejson.{Int => _, _}
import quasar.Predef._
import quasar.fp._

import matryoshka._, Recursive.ops._
import monocle.macros.Lenses
import scalaz._, Scalaz._

sealed abstract class QScriptBucket[T[_[_]], A] {
  def src: A
}

@Lenses final case class GroupBy[T[_[_]], A](
  src: A,
  values: FreeMap[T],
  bucket: FreeMap[T])
    extends QScriptBucket[T, A]

@Lenses final case class BucketField[T[_[_]], A](
  src: A,
  value: FreeMap[T],
  name: FreeMap[T])
    extends QScriptBucket[T, A]

@Lenses final case class BucketIndex[T[_[_]], A](
  src: A,
  value: FreeMap[T],
  index: FreeMap[T])
    extends QScriptBucket[T, A]

@Lenses final case class LeftShiftBucket[T[_[_]], A](
  src: A,
  struct: FreeMap[T],
  repair: JoinFunc[T],
  bucketShift: FreeMap[T])
    extends QScriptBucket[T, A]

@Lenses final case class SquashBucket[T[_[_]], A](
  src: A)
    extends QScriptBucket[T, A]

object QScriptBucket {
  implicit def equal[T[_[_]]](implicit eqTEj: Equal[T[EJson]]): Delay[Equal, QScriptBucket[T, ?]] =
    new Delay[Equal, QScriptBucket[T, ?]] {
      def apply[A](eq: Equal[A]) = ???
    }

  implicit def traverse[T[_[_]]]: Traverse[QScriptBucket[T, ?]] =
    new Traverse[QScriptBucket[T, ?]] {
      def traverseImpl[G[_], A, B](
        fa: QScriptBucket[T, A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[QScriptBucket[T, B]] = fa match {
        case GroupBy(src, values, bucket) => f(src) ∘ (GroupBy(_, values, bucket))
        case BucketField(src, values, name) => f(src) ∘ (BucketField(_, values, name))
        case BucketIndex(src, values, index) => f(src) ∘ (BucketIndex(_, values, index))
        case LeftShiftBucket(src, struct, repair, bucket) =>
          f(src) ∘ (LeftShiftBucket(_, struct, repair, bucket))
        case SquashBucket(src) => f(src) ∘ (SquashBucket(_))
      }
    }

  implicit def show[T[_[_]]](implicit shEj: Show[T[EJson]]): Delay[Show, QScriptBucket[T, ?]] =
    new Delay[Show, QScriptBucket[T, ?]] {
      def apply[A](s: Show[A]): Show[QScriptBucket[T, A]] = ???
    }

  implicit def mergeable[T[_[_]]: Corecursive]:
      Mergeable.Aux[T, QScriptBucket[T, Unit]] =
    new Mergeable[QScriptBucket[T, Unit]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: QScriptBucket[IT, Unit],
        p2: QScriptBucket[IT, Unit]): Option[Merge[IT, QScriptBucket[IT, Unit]]] = ???
    }

  implicit def bucketable[T[_[_]]: Corecursive]:
      Bucketable.Aux[T, QScriptBucket[T, ?]] =
    new Bucketable[QScriptBucket[T, ?]] {
      type IT[G[_]] = T[G]

      // Int is number of buckets to skip
      def digForBucket: QScriptBucket[T, Inner] => StateT[QScriptBucket[T, Inner] \/ ?, Int, Inner] = {
        case LeftShiftBucket(src, struct, repair, bucket) => StateT { s =>
          if (s == 0)
            LeftShiftBucket(src, rebase(struct, bucket), repair, bucket).left[(Int, Inner)]
          else
            ((s - 1, src)).right[QScriptBucket[T, Inner]]
        }
        case gb @ GroupBy(src, _, _) => StateT { s =>
          if (s == 0)
            gb.left[(Int, Inner)]
          else
            ((s - 1, src)).right[QScriptBucket[T, Inner]]
        }
        case bf @ BucketField(src, _, _) => StateT { s =>
          if (s == 0)
            bf.left[(Int, Inner)]
          else
            ((s - 1, src)).right[QScriptBucket[T, Inner]]
        }
        case bi @ BucketIndex(src, _, _) => StateT { s =>
          if (s == 0)
            bi.left[(Int, Inner)]
          else
            ((s - 1, src)).right[QScriptBucket[T, Inner]]
        }
        case sq @ SquashBucket(_) => StateT { s =>
          sq.left[(Int, Inner)]
        }
      }
    }

  def Z[T[_[_]]] = scala.Predef.implicitly[SourcedPathable[T, ?] :<: QScriptPure[T, ?]]

  implicit def elideBuckets[T[_[_]]: Recursive]:
      ElideBuckets.Aux[T, QScriptBucket[T, ?]] =
    new ElideBuckets[QScriptBucket[T, ?]] {
      type IT[G[_]] = T[G]

      def purify: QScriptBucket[T, InnerPure] => QScriptPure[IT, InnerPure] = {
        case GroupBy(src, values, _) => Z[T].inj(Map(src, values))
        case BucketField(src, value, name) => Z[T].inj(Map(src, Free.roll(MapFuncs.ProjectField(name, value))))
        case BucketIndex(src, value, index) => Z[T].inj(Map(src, Free.roll(MapFuncs.ProjectIndex(index, value))))
        case LeftShiftBucket(src, struct, repair, _) => Z[T].inj(LeftShift(src, struct, repair))
        case SquashBucket(src) => src.project
      }
    }
}
