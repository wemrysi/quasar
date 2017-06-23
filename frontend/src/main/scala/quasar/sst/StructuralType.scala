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
import quasar.contrib.scalaz.zipper._
import quasar.ejson.{EJson, EncodeEJson, EncodeEJsonK}
import quasar.fp.ski.κ
import quasar.tpe._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import monocle.Lens
import scalaz._, Scalaz._

/** A measure annotated Type focused on structure over semantics.
  *
  * @tparam L the type of literals
  * @tparam V the measure the type is annotated with
  */
final case class StructuralType[L, V](toCofree: Cofree[TypeF[L, ?], V]) extends scala.AnyVal {
  def cobind[W](f: StructuralType[L, V] => W): StructuralType[L, W] =
    StructuralType(toCofree.cobind(c => f(StructuralType(c))))

  def cojoin: StructuralType[L, StructuralType[L, V]] =
    StructuralType(toCofree.duplicate map (StructuralType(_)))

  def copoint: V =
    toCofree.head

  def map[W](f: V => W): StructuralType[L, W] =
    StructuralType(toCofree map f)

  /** Convert to a type, forgetting annotations. */
  def toType[T](implicit T: Corecursive.Aux[T, TypeF[L, ?]]): T =
    forgetAnnotation[Cofree[TypeF[L, ?], V], T, TypeF[L, ?], V](toCofree)
}

object StructuralType extends StructuralTypeInstances {
  /** Lens targeting the top-level annotation of a structural type. */
  def measure[L, V]: Lens[StructuralType[L, V], V] =
    Lens[StructuralType[L, V], V](_.copoint)(v => st => envT(v, st.project.lower).embed)

  /** The structural type representing the given EJson value, annotated with
    * the result of `measure`.
    */
  object fromEJson {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[V](measure: Algebra[TypeF[J, ?], V], ejson: J)(
        implicit
        J : Order[J],
        JC: Corecursive.Aux[J, EJson],
        JR: Recursive.Aux[J, EJson]
      ): StructuralType[J, V] = {
        type T = Fix[TypeF[J, ?]]

        val attrV =
          attributeAlgebra[TypeF[J, ?], V](measure)

        val normAndAttr =
          ((_: T).cata[Cofree[TypeF[J, ?], V]](attrV)) <<<
          ((_: TypeF[J, T]).embed)                     <<<
          normalization.lowerConst[J, T]

        StructuralType(normAndAttr(TypeF.const[J, T](ejson)))
      }
    }
  }

  /** The structural type representing the given EJson value annotated with
    * the given measure.
    */
  object fromEJsonK {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[V](measure: V, ejson: J)(
        implicit
        J : Order[J],
        JC: Corecursive.Aux[J, EJson],
        JR: Recursive.Aux[J, EJson]
      ): StructuralType[J, V] =
        fromEJson[J](κ(measure), ejson)
    }
  }

  /** Unfold a pair of structural types into their deep structural merge. */
  def mergeƒ[L, V: Monoid, F[_]: Functor, T](
    implicit
    I: TypeF[L, ?] :<: F,
    F: StructuralMerge[F],
    TC: Corecursive.Aux[T, EnvT[V, F, ?]],
    TR: Recursive.Aux[T, EnvT[V, F, ?]]
  ): ElgotCoalgebra[T \/ ?, EnvT[V, F, ?], (T, T)] =
    tt => {
      val pp = tt.umap(_.project)
      F.merge[V, T](pp) getOrElse (pp match {
        case (EnvT((v, _)), EnvT((w, _))) =>
          envT(v |+| w, I(TypeF.coproduct[L, T](tt))).embed.left
      })
    }

  /** A transform ensuring unions are disjoint by merging their members. */
  def disjoinUnionsƒ[L, V: Monoid, F[_]: Functor, T](
    implicit
    I: TypeF[L, ?] :<: F,
    F: StructuralMerge[F],
    TC: Corecursive.Aux[T, EnvT[V, F, ?]],
    TR: Recursive.Aux[T, EnvT[V, F, ?]]
  ): EnvT[V, F, T] => EnvT[V, F, T] = {
    def strictMerge(x: T, y: T): Option[T] =
      some((x, y).elgotApo[T](mergeƒ[L, V, F, T])) filter { t =>
        I.prj(t.project.lower) all TypeF.union[L, T].isEmpty
      }

    orOriginal(ft => I.prj(ft.lower) collect {
      case TypeF.Unioned(NonEmptyList(h, t)) =>
        val disjoint = t.foldLeft(h.wrapNel.toZipper) { (z, t) =>
          z.findMap(strictMerge(t, _)) getOrElse z.insert(t)
        }

        disjoint.toNel match {
          case NonEmptyList(a, INil()) =>
            a.project

          case NonEmptyList(a, ICons(b, cs)) =>
            envT(ft.ask, I(TypeF.union[L, T](a, b, cs)))
        }
    })
  }
}

sealed abstract class StructuralTypeInstances extends StructuralTypeInstances0 {
  implicit def encodeEJson[L: EncodeEJson, V: EncodeEJson]: EncodeEJson[StructuralType[L, V]] = {
    implicit val encodeEnvT = EncodeEJsonK.envT[V, TypeF[L, ?]]("measure", "structure")
    EncodeEJson.encodeEJsonR[StructuralType[L, V], EnvT[V, TypeF[L, ?], ?]]
  }

  implicit def corecursive[L, V]: Corecursive.Aux[StructuralType[L, V], EnvT[V, TypeF[L, ?], ?]] =
    new Corecursive[StructuralType[L, V]] {
      type Base[B] = EnvT[V, TypeF[L, ?], B]

      def embed(st: EnvT[V, TypeF[L, ?], StructuralType[L, V]])(implicit BF: Functor[EnvT[V, TypeF[L, ?], ?]]) =
        StructuralType(st.map(_.toCofree).embed)
    }

  implicit def recursive[L, V]: Recursive.Aux[StructuralType[L, V], EnvT[V, TypeF[L, ?], ?]] =
    new Recursive[StructuralType[L, V]] {
      type Base[B] = EnvT[V, TypeF[L, ?], B]

      def project(st: StructuralType[L, V])(implicit BF: Functor[EnvT[V, TypeF[L, ?], ?]]) =
        st.toCofree.project map (StructuralType(_))
    }

  implicit def monoid[L: Order, V: Monoid]: Monoid[StructuralType[L, V]] =
    new Monoid[StructuralType[L, V]] {
      type F[A] = TypeF[L, A]
      type T    = Cofree[F, V]
      val norm = (_: T).transAna[T](StructuralType.disjoinUnionsƒ[L, V, F, T])
      val zero = StructuralType(Cofree(∅[V], TypeF.bottom[L, T]()))
      def append(x: StructuralType[L, V], y: => StructuralType[L, V]) =
        StructuralType((x.toCofree, y.toCofree).elgotApo[T](
          StructuralType.mergeƒ[L, V, F, T] >>> (_ leftMap norm)))
    }

  implicit def comonad[L]: Comonad[StructuralType[L, ?]] =
    new Comonad[StructuralType[L, ?]] {
      def copoint[A](st: StructuralType[L, A]) =
        st.copoint

      override def cojoin[A](st: StructuralType[L, A]) =
        st.cojoin

      override final def map[A, B](st: StructuralType[L, A])(f: A => B) =
        st map f

      def cobind[A, B](st: StructuralType[L, A])(f: StructuralType[L, A] => B) =
        st cobind f
    }

  implicit def equal[L: Equal, V: Equal]: Equal[StructuralType[L, V]] = {
    implicit val eqlTypeF = TypeF.structuralEqual[L]
    Equal.equalBy(_.toCofree)
  }

  implicit def show[L: Show, V: Show]: Show[StructuralType[L, V]] =
    Show.show(st => Cord("StructuralType") ++ st.toCofree.show)
}

sealed abstract class StructuralTypeInstances0 {
  implicit def traverse1[L]: Traverse1[StructuralType[L, ?]] =
    new Traverse1[StructuralType[L, ?]] {
      override def map[A, B](st: StructuralType[L, A])(f: A => B): StructuralType[L, B] =
        StructuralType(st.toCofree map f)

      override final def foldMap[A, B: Monoid](st: StructuralType[L, A])(f: A => B): B =
        st.toCofree.foldMap(f)

      override final def foldRight[A, B](st: StructuralType[L, A], z: => B)(f: (A, => B) => B): B =
        st.toCofree.foldRight(z)(f)

      override final def foldLeft[A, B](st: StructuralType[L, A], z: B)(f: (B, A) => B): B =
        st.toCofree.foldLeft(z)(f)

      override final def foldMapLeft1[A, B](st: StructuralType[L, A])(z: A => B)(f: (B, A) => B): B =
        st.toCofree.foldMapLeft1(z)(f)

      override def foldMapRight1[A, B](st: StructuralType[L, A])(z: A => B)(f: (A, => B) => B): B =
        st.toCofree.foldMapRight1(z)(f)

      override def foldMap1[A, B](st: StructuralType[L, A])(f: A => B)(implicit S: Semigroup[B]): B =
        st.toCofree.foldMap1(f)

      override def traverseImpl[G[_]: Applicative, A, B](st: StructuralType[L, A])(f: A => G[B]): G[StructuralType[L, B]] =
        st.toCofree.traverse(f) map (StructuralType(_))

      def traverse1Impl[G[_]: Apply, A, B](st: StructuralType[L, A])(f: A => G[B]): G[StructuralType[L, B]] =
        st.toCofree.traverse1(f) map (StructuralType(_))
    }
}
