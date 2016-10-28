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
import scala.{ Null, collection => sc }

trait JEncoder[-A] {
  type Json
  def apply(value: A): Json
}
object JEncoder {
  type Aux[-A, J] = JEncoder[A] { type Json = J }

  /** This is used by the encoding macro, which supplies explicit
   *  type arguments.
   */
  def lift[A, J](value: A)(implicit z: Aux[A, J]): J = z(value)

  def make[A, J](f: A => J): Aux[A, J] = new Impl[A, J](f)

  def apply[A](implicit z: JEncoder[A]): Aux[A, z.Json] = z

  implicit def liftIdentity[J] : Aux[J, J]                                  = make(x => x)
  implicit def liftBool[J](implicit z: Facade[J]): Aux[Boolean, J]          = make(x => if (x) z.jtrue else z.jfalse)
  implicit def liftNull[J](implicit z: Facade[J]): Aux[Null, J]             = make(_ => z.jnull)
  implicit def liftInt[J](implicit z: Facade[J]): Aux[Int, J]               = make(z jint _.toString)
  implicit def liftLong[J](implicit z: Facade[J]): Aux[Long, J]             = make(z jint _.toString)
  implicit def liftBigInt[J](implicit z: Facade[J]): Aux[BigInt, J]         = make(z jint _.toString)
  implicit def liftDouble[J](implicit z: Facade[J]): Aux[Double, J]         = make(z jnum _.toString)
  implicit def liftBigDecimal[J](implicit z: Facade[J]): Aux[BigDecimal, J] = make(z jnum _.toString)
  implicit def liftString[J](implicit z: Facade[J]): Aux[String, J]         = make(z jstring _)

  implicit def liftMap[V, J](implicit zf: Facade[J], zl: Aux[V, J]): Aux[sc.Map[String, V], J] = make { xs =>
    val fcontext = zf.objectContext()
    xs.keysIterator foreach { k =>
      fcontext add k
      fcontext add zl(xs(k))
    }
    fcontext.finish
  }
  implicit def liftArray[V, J](implicit zf: Facade[J], zl: Aux[V, J]): Aux[Array[V], J] = make { xs =>
    val fcontext = zf.arrayContext()
    xs foreach (x => fcontext add zl(x))
    fcontext.finish
  }
  implicit def liftSeq[V, J](implicit zf: Facade[J], zl: Aux[V, J]): Aux[sc.Seq[V], J] = make { xs =>
    val fcontext = zf.arrayContext()
    xs foreach (x => fcontext add zl(x))
    fcontext.finish
  }

  final class Impl[A, J](f: A => J) extends JEncoder[A] {
    type Json = J
    def apply(value: A): Json = f(value)
  }
}
