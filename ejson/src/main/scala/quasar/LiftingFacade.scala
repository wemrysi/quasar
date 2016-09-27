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

package quasar

import quasar.Predef._
import jawn.Facade
import scala.{ Function1, Null, collection => sc }
import scalaz.~>

trait JEncoder[A] {
  type Json
  def apply(value: A): Json
}
object JEncoder {
  class Aux[A, J](f: A => J) extends JEncoder[A] {
    type Json = J
    def apply(value: A): Json = f(value)
  }

  def apply[A](implicit z: JEncoder[A]): JEncoder[A] { type Json = z.Json } = z
}
trait JLift[A] {
  def lift[J](value: A)(implicit z: Facade[J]): J
}
object JLift {
  def liftJson[A, J](value: A)(implicit zl: JLift[A], zf: Facade[J]): J = zl.lift[J](value)

  private def make[A](f: Facade ~> scala.Function1[A, ?]): JLift[A] = new JLift[A] {
    def lift[J](value: A)(implicit z: Facade[J]): J = f(z)(value)
  }

  implicit val liftBool       = make[Boolean](λ[Facade ~> Function1[Boolean, ?]](z => x => if (x) z.jtrue() else z.jfalse()))
  implicit val liftNull       = make[Null](λ[Facade ~> Function1[Null, ?]](z => x => z.jnull()))
  implicit val liftInt        = make[Int](λ[Facade ~> Function1[Int, ?]](z => x => z jint x.toString))
  implicit val liftLong       = make[Long](λ[Facade ~> Function1[Long, ?]](z => x => z jint x.toString))
  implicit val liftBigInt     = make[BigInt](λ[Facade ~> Function1[BigInt, ?]](z => x => z jint x.toString))
  implicit val liftDouble     = make[Double](λ[Facade ~> Function1[Double, ?]](z => x => z jnum x.toString))
  implicit val liftBigDecimal = make[BigDecimal](λ[Facade ~> Function1[BigDecimal, ?]](z => x => z jnum x.toString))
  implicit val liftString     = make[String](λ[Facade ~> Function1[String, ?]](z => x => z jstring x))

  implicit def liftMap[A, M[K, V] <: sc.Map[K, V]](implicit zl: JLift[A]): JLift[M[String, A]] = new JLift[M[String, A]] {
    def lift[J](m: M[String, A])(implicit z: Facade[J]): J = {
      val fcontext = z.objectContext()
      for ((k, v) <- m) {
        fcontext add k
        fcontext add liftJson(v)
      }
      fcontext.finish
    }
  }
  implicit def liftArray[A](implicit zl: JLift[A]): JLift[Array[A]] = new JLift[Array[A]] {
    def lift[J](xs: Array[A])(implicit zf: Facade[J]): J = {
      val fcontext = zf.arrayContext()
      xs foreach (x => fcontext add liftJson(x))
      fcontext.finish
    }
  }
  implicit def liftSeq[A, M[X] <: sc.Seq[X]](implicit zl: JLift[A]): JLift[M[A]] = new JLift[M[A]] {
    def lift[J](xs: M[A])(implicit zf: Facade[J]): J = {
      val fcontext = zf.arrayContext()
      xs foreach (x => fcontext add liftJson(x))
      fcontext.finish
    }
  }
}
