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

package quasar.qsu

import slamdata.Predef.Symbol
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.qscript.{FreeMap, FreeMapA}

import matryoshka.{delayShow, delayEqual, BirecursiveT, ShowT, EqualT}
import matryoshka.data._
import scalaz.{\/, Cord, Equal, IMap, ISet, Monoid, Order, Show}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.applicative._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.option._
import matryoshka.{Hole => _, _}
import matryoshka.data._

final case class References[T[_[_]], A](
    accessing: References.Accessing[T, A],
    accessed: References.Accessed[A]) {

  def modifyAccess(of: Access[A, Symbol])(f: FreeMap[T] => FreeMap[T])
      (implicit A: Order[A])
      : References[T, A] =
    modifySomeAccess(of, ISet.empty)(f)

  def modifySomeAccess(of: Access[A, Symbol], except: ISet[Symbol])(f: FreeMap[T] => FreeMap[T])
      (implicit A: Order[A])
      : References[T, A] =
    accessed.lookup(of).fold(this) { syms =>
      copy(accessing = (syms \\ except).foldLeft(accessing)(_.adjust(_, _.adjust(of, f))))
    }

  def recordAccess(by: Symbol, of: Access[A, Symbol], as: FreeMap[T])
      (implicit A: Order[A])
      : References[T, A] = {
    val accesses = IMap.singleton(of, as)
    val accessing1 = accessing.alter(by, _ map (_ union accesses) orElse some(accesses))
    val accessed1 = accessed |+| IMap.singleton(of, ISet singleton by)
    References(accessing1, accessed1)
  }

  /** Replace all accesses by `prev` with `next`. */
  def replaceAccess(prev: Symbol, next: Symbol)(implicit A: Order[A]): References[T, A] =
    accessing.lookup(prev).fold(this) { accesses =>
      val nextAccessed = accesses.keySet.foldLeft(accessed) { (m, a) =>
        m.adjust(a, _.delete(prev).insert(next))
      }

      References(accessing.delete(prev).insert(next, accesses), nextAccessed)
    }

  /** Resolves all `Access` made by `by` in the given `FreeMap`, resulting in a `FreeMap`. */
  def resolveAccess[B, C]
    (by: Symbol, in: FreeMapA[T, B])
    (f: C => Symbol)
    (ex: B => Access[A, C] \/ C)
    (implicit A: Order[A])
    : FreeMapA[T, C] =
      accessing.lookup(by).fold(in.map(b => ex(b).fold(Access.Optics[A].src.get(_), c => c))) { accesses =>
        in flatMap { b =>
          ex(b).fold({ access =>
            val c = Access.Optics[A].src.get(access)
            accesses.lookup(access.symbolic(f)).cata(_.as(c), c.point[FreeMapA[T, ?]])
          }, {
            _.point[FreeMapA[T, ?]]
          })
        }
      }
}

object References {
  // How to access a given identity or value.
  type Accesses[T[_[_]], A] = IMap[Access[A, Symbol], FreeMap[T]]
  // Accesses by vertex.
  type Accessing[T[_[_]], A] = IMap[Symbol, Accesses[T, A]]
  // Vertices by access.
  type Accessed[A] = IMap[Access[A, Symbol], ISet[Symbol]]

  def noRefs[T[_[_]], A]: References[T, A] =
    References(IMap.empty, IMap.empty)

  implicit def monoid[T[_[_]], A: Order]: Monoid[References[T, A]] =
    Monoid.instance(
      (x, y) => References(
        x.accessing.unionWith(y.accessing)(_ union _),
        x.accessed |+| y.accessed),
      noRefs)

  implicit def equal[T[_[_]]: BirecursiveT: EqualT, A: Equal]: Equal[References[T, A]] =
    Equal.equalBy(r => (r.accessing, r.accessed))

  implicit def show[T[_[_]]: ShowT, A: Show]: Show[References[T, A]] =
    Show.show {
      case References(accessing, accessed) =>
        Cord("References {\n\n") ++
        Cord("Accessing[\n") ++
        Cord(printMultiline(accessing.toList)) ++
        Cord("]\n\nAccessed[\n") ++
        Cord(printMultiline(accessed.toList)) ++
        Cord("]\n}")
    }
}
