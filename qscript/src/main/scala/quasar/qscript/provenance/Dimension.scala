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

package quasar.qscript.provenance

import slamdata.Predef._
import quasar.fp.ski.ι

import matryoshka._
import scalaz._, Scalaz._
import scalaz.Tags.Conjunction

trait Dimension[D, I, P] {
  val prov: Prov[D, I, P]

  import prov._
  import prov.implicits._

  /** Returns the `JoinKeys` describing the autojoin of the two dimensions. */
  def autojoinKeys(ls: Dimensions[P], rs: Dimensions[P])(implicit D: Equal[D]): JoinKeys[I] =
    ls.union.tuple(rs.union) foldMap {
      case (l, r) =>
        val joined =
          l.reverse.alignBoth(r.reverse).foldLeftM(true) { (b, lr) =>
            if (b)
              lr anyM { case (a, b) => autojoined[Writer[JoinKeys[I], ?]](a, b) }
            else
              false.point[Writer[JoinKeys[I], ?]]
          }

        joined.written
          .keys.foldMap1Opt(ι)
          .fold(JoinKeys.empty[I])(single => JoinKeys(IList(single)))
    }

  /** The empty dimension stack. */
  val empty: Dimensions[P] =
    Dimensions.empty[P]

  /** Updates the dimensional stack by sequencing a new dimension from value
    * space with the top dimension.
    */
  def flatten(id: I, ds: Dimensions[P]): Dimensions[P] =
    nest(lshift(id, ds))

  /** Inject a value into a structure at an unknown field. */
  val injectDynamic: Dimensions[P] => Dimensions[P] =
    Dimensions.topDimension.modify(fresh() ≺: _)

  /** Inject a value into a structure at the given field. */
  def injectStatic(field: D, ds: Dimensions[P]): Dimensions[P] =
    Dimensions.topDimension[P].modify(injValue(field) ≺: _)(ds)

  /** Joins two dimensions into a single dimension stack, starting from the base. */
  def join(ls: Dimensions[P], rs: Dimensions[P])(implicit D: Equal[D], I: Equal[I]): Dimensions[P] =
    Conjunction.unsubst(Conjunction.subst(ls) ∧ Conjunction.subst(rs))

  /** Shifts the dimensional stack by pushing a new dimension from value space
    * onto the stack.
    */
  def lshift(id: I, ds: Dimensions[P]): Dimensions[P] =
    ds.mapJoin(value(id) <:: _)

  /** Sequences the top and preceding dimensions. */
  val nest: Dimensions[P] => Dimensions[P] =
    Dimensions.join[P] modify {
      case NonEmptyList(a, ICons(b, cs)) => NonEmptyList.nel(a ≺: b, cs)
      case other => other
    }

  /** Project an unknown field from a value-level structure. */
  val projectDynamic: Dimensions[P] => Dimensions[P] =
    Dimensions.topDimension.modify(fresh() ≺: _)

  /** Project a static path segment. */
  def projectPath(segment: D, ds: Dimensions[P]): Dimensions[P] =
    if (ds.isEmpty)
      Dimensions.origin(prjPath(segment))
    else
      Dimensions.topDimension[P].modify(prjPath(segment) ≺: _)(ds)

  /** Project a static field from value-level structure. */
  def projectStatic(field: D, ds: Dimensions[P])(implicit D: Equal[D], I: Equal[I]): Dimensions[P] =
    Dimensions.union[P].modify(_ flatMap { jn =>
      applyProjection(prjValue(field) ≺: jn.head) match {
        case Success(Some(p)) => IList(NonEmptyList.nel(p, jn.tail))
        case Success(None) => jn.tail.toNel.toIList
        case Failure(_) => IList()
      }
    })(ds)

  /** Remove the top dimension from the stack. */
  val reduce: Dimensions[P] => Dimensions[P] =
    Dimensions.union[P].modify(_.flatMap(_.tail.toNel.toIList))

  /** Collapses all dimensions into a single one. */
  val squash: Dimensions[P] => Dimensions[P] =
    Dimensions.join[P].modify(_.foldMap1(ι).wrapNel)

  /** Swaps the dimensions at the nth and mth indices. */
  def swap(idxN: Int, idxM: Int, ds: Dimensions[P]): Dimensions[P] = {
    val n = if (idxN < idxM) idxN else idxM
    val m = if (idxM > idxN) idxM else idxN

    ds mapJoin { jn =>
      val swapped = for {
        z0  <- some(jn.toZipper)
        z0n <- z0.move(n)
        vn  =  z0n.focus
        z0m <- z0n.move(m - n)
        vm  =  z0m.focus
        z1  =  z0m.update(vn)
        z1n <- z1.move(n - m)
        z2  =  z1n.update(vm)
      } yield z2.toNel

      swapped getOrElse jn
    }
  }

  /** Disjoin two dimension stacks. */
  def union(ls: Dimensions[P], rs: Dimensions[P])(implicit D: Equal[D], I: Equal[I]): Dimensions[P] =
    ls ∨ rs
}

object Dimension {
  def apply[D, I, P](prov0: Prov[D, I, P]): Dimension[D, I, P] =
    new Dimension[D, I, P] { val prov = prov0 }
}
