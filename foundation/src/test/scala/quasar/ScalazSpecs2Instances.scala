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

import org.scalacheck.Gen
import org.specs2.specification.core.Fragments

trait ScalazSpecs2Instances extends org.specs2.scalacheck.GenInstances {

  import org.specs2.execute.Result

  import scalaz.{ Monad, Monoid }

  implicit def scalazGenMonad: Monad[Gen] = new Monad[Gen] {
    private val specsMonad = genMonad
    def point[A](a: =>A): Gen[A] = specsMonad.point(a)
    def bind[A, B](fa: Gen[A])(f: A => Gen[B]): Gen[B] = specsMonad.bind(fa)(f)
  }

  implicit val ScalazResultFailureMonoid: Monoid[Result] = new Specs2ToScalazMonoid(Result.ResultFailureMonoid)

  val ScalazResultMonoid: Monoid[Result] = new Specs2ToScalazMonoid(Result.ResultMonoid)

  implicit def FragmentsMonoid: Monoid[Fragments] = new Specs2ToScalazMonoid[Fragments](Fragments.FragmentsMonoid)

  private class Specs2ToScalazMonoid[A](private val specsMonoid: org.specs2.fp.Monoid[A]) extends Monoid[A] {
    override def zero: A = specsMonoid.zero
    override def append(f1: A, f2: => A): A = specsMonoid.append(f1, f2)
  }

}
