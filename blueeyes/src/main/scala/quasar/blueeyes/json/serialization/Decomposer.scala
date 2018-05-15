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

package quasar.blueeyes.json.serialization

import quasar.blueeyes.json._

import scalaz._

/** Decomposes the value into a JSON object.
  */
trait Decomposer[A] { self =>
  def decompose(tvalue: A): JValue

  def contramap[B](f: B => A): Decomposer[B] = new Decomposer[B] {
    override def decompose(b: B) = self.decompose(f(b))
  }

  def apply(tvalue: A): JValue = decompose(tvalue)

  def unproject(jpath: JPath): Decomposer[A] = new Decomposer[A] {
    override def decompose(b: A) = JUndefined.unsafeInsert(jpath, self.decompose(b))
  }
}

object Decomposer {
  def apply[A](implicit d: Decomposer[A]): Decomposer[A] = d
}

/** Serialization implicits allow a convenient syntax for serialization and
  * deserialization when implicit decomposers and extractors are in scope.
  * <p>
  * foo.serialize
  * <p>
  * jvalue.deserialize[Foo]
  */
trait SerializationImplicits {
  case class DeserializableJValue(jvalue: JValue) {
    def deserialize[T](implicit e: Extractor[T]): T                            = e.extract(jvalue)
    def validated[T](implicit e: Extractor[T]): Validation[Extractor.Error, T] = e.validated(jvalue)
    def validated[T](jpath: JPath)(implicit e: Extractor[T])                   = e.validated(jvalue, jpath)
  }

  case class SerializableTValue[T](tvalue: T) {
    def serialize(implicit d: Decomposer[T]): JValue = d.decompose(tvalue)
    def jv(implicit d: Decomposer[T]): JValue        = d.decompose(tvalue)
  }

  implicit def JValueToTValue[T](jvalue: JValue): DeserializableJValue = DeserializableJValue(jvalue)
  implicit def TValueToJValue[T](tvalue: T): SerializableTValue[T]     = SerializableTValue[T](tvalue)
}

object SerializationImplicits extends SerializationImplicits

/** Bundles default extractors, default decomposers, and serialization
  * implicits for natural serialization of core supported types.
  */
object DefaultSerialization extends DefaultExtractors with DefaultDecomposers with SerializationImplicits
