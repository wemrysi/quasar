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

import quasar.blueeyes._, json._
import quasar.precog._
import scalaz._, Scalaz._, Validation._

import scala.reflect.ClassTag

/** Extracts the value from a JSON object. You must implement either validated or extract.
  */
trait Extractor[A] { self =>
  import Extractor._

  def extract(jvalue: JValue): A = validated(jvalue) valueOr {
    case Thrown(ex) => throw new IllegalArgumentException("Unable to deserialize " + jvalue, ex)
    case other      => throw new IllegalArgumentException("Unable to deserialize " + jvalue + ": " + other.message)
  }

  def validated(jvalue: JValue): Validation[Error, A]
  def validated(jvalue: JValue, jpath: JPath): Validation[Error, A] =
    ((cause: Extractor.Error) => Extractor.Invalid("Unable to deserialize property or child " + jpath, Some(cause))) <-: validated(jvalue.get(jpath))

  def apply(jvalue: JValue): A = extract(jvalue)
}

object Extractor {
  def apply[A](implicit e: Extractor[A]): Extractor[A] = e

  def invalidv[A](message: String) = failure[Error, A](Invalid(message))
  def tryv[A](a: => A)             = (Thrown.apply _) <-: Validation.fromTryCatchNonFatal(a)

  sealed trait Error {
    def message: String
  }

  object Error {
    def thrown(exception: Throwable): Error         = Thrown(exception)
    def invalid(message: String): Error             = Invalid(message)
    def combine(errors: NonEmptyList[Error]): Error = Errors(errors)

    implicit object Semigroup extends scalaz.Semigroup[Error] {
      import NonEmptyList._
      def append(e1: Error, e2: => Error): Error = (e1, e2) match {
        case (Errors(l1), Errors(l2)) => Errors(l1.list <::: l2)
        case (Errors(l), x)           => Errors(nel(x, l.list))
        case (x, Errors(l))           => Errors(x <:: l)
        case (x, y)                   => Errors(nels(x, y))
      }
    }
  }

  case class Invalid(msg: String, cause: Option[Error] = None) extends Error {
    def message =
      cause map { c =>
        "%s (caused by %s)".format(msg, c.message)
      } getOrElse msg
  }

  case class Thrown(exception: Throwable) extends Error {
    def message: String = exception.getMessage
  }

  case class Errors(errors: NonEmptyList[Error]) extends Error {
    def message = "Multiple extraction errors occurred: " + errors.map(_.message).list.toList.mkString(": ")
  }

  def apply[A: ClassTag](f: PartialFunction[JValue, A]): Extractor[A] = new Extractor[A] {
    def validated(jvalue: JValue) = {
      if (f.isDefinedAt(jvalue)) Success(f(jvalue))
      else Failure(Invalid("Extraction not defined from value " + jvalue + " to type " + implicitly[ClassTag[A]].runtimeClass.getName))
    }
  }
}
