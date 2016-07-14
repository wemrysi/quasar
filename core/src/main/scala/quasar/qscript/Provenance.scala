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

import quasar.Predef._
import quasar.fp._
import quasar.qscript.MapFuncs._

import matryoshka._
import scalaz._, Scalaz._

// NB: Should we use char lits instead?
class Provenance[T[_[_]]: Corecursive] {
  private def tagIdentity[A](tag: String, mf: Free[MapFunc[T, ?], A]) =
    Free.roll(MakeMap[T, Free[MapFunc[T, ?], A]](StrLit(tag), mf))

  // provenances:
  // projectfield: f
  // projectindex: i
  // join:         j []
  // nest:         n []
  // shiftmap:     m
  // shiftarray:   a
  // unionleft:    l
  // unionright:   r
  def projectField[A](mf: Free[MapFunc[T, ?], A]) = tagIdentity("f", mf)
  def projectIndex[A](mf: Free[MapFunc[T, ?], A]) = tagIdentity("i", mf)
  def shiftMap[A](mf: Free[MapFunc[T, ?], A]) = tagIdentity("m", mf)
  def shiftArray[A](mf: Free[MapFunc[T, ?], A]) = tagIdentity("a", mf)
  def unionLeft[A](mf: Free[MapFunc[T, ?], A]) = tagIdentity("l", mf)
  def unionRight[A](mf: Free[MapFunc[T, ?], A]) = tagIdentity("r", mf)
  def join[A](left: Free[MapFunc[T, ?], A], right: Free[MapFunc[T, ?], A]) =
    tagIdentity("j",
      Free.roll(ConcatArrays(
        Free.roll(MakeArray[T, Free[MapFunc[T, ?], A]](left)),
        Free.roll(MakeArray[T, Free[MapFunc[T, ?], A]](right)))))
  def nest[A](car: Free[MapFunc[T, ?], A], cadr: Free[MapFunc[T, ?], A]) =
    tagIdentity("n",
      Free.roll(ConcatArrays(
        Free.roll(MakeArray[T, Free[MapFunc[T, ?], A]](car)),
        Free.roll(MakeArray[T, Free[MapFunc[T, ?], A]](cadr)))))


  def joinProvenances(leftBuckets: List[FreeMap[T]], rightBuckets: List[FreeMap[T]]):
      List[JoinFunc[T]] =
    leftBuckets.alignWith(rightBuckets) {
      case \&/.Both(l, r) => join(l.map(κ(LeftSide)), r.map(κ(RightSide)))
      case \&/.This(l)    => join(l.map(κ(LeftSide)), NullLit())
      case \&/.That(r)    => join(NullLit(), r.map(κ(RightSide)))
    }

  def unionProvenances(leftBuckets: List[FreeMap[T]], rightBuckets: List[FreeMap[T]]):
      (List[FreeMap[T]], List[FreeMap[T]]) =
    leftBuckets.alignWith(rightBuckets) {
      case \&/.Both(l, r) => (l, r)
      case \&/.This(l)    => (l, NullLit[T, Unit]())
      case \&/.That(r)    => (NullLit[T, Unit](), r)
    }.map(_.bimap(unionLeft[Unit], unionRight[Unit])).unzip


  def nestProvenances(buckets: List[FreeMap[T]]): List[FreeMap[T]] =
    buckets match {
      case a :: b :: tail => nest(a, b) :: tail
      case _              => buckets
    }

  def squashProvenances(buckets: List[FreeMap[T]]): List[FreeMap[T]] =
    buckets match {
      case a :: b :: tail => squashProvenances(nest(a, b) :: tail)
      case _              => buckets
    }
}
