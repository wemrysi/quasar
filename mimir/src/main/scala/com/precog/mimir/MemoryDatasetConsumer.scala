/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.mimir

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.yggdrasil.execution.EvaluationContext
import scalaz._, Scalaz._
import blueeyes._, json._

trait MemoryDatasetConsumer[M[+ _]] extends EvaluatorModule[M] {
  type IdType

  type X      = Throwable
  type SEvent = (Vector[IdType], SValue)

  implicit def M: Monad[M] with Comonad[M]

  def Evaluator[N[+ _]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M): EvaluatorLike[N]

  def extractIds(jv: JValue): Seq[IdType]

  def consumeEval(graph: DepGraph, ctx: EvaluationContext, optimize: Boolean = true): Validation[X, Set[SEvent]] = {
    Validation.fromTryCatchNonFatal {
      implicit val nt = NaturalTransformation.refl[M]
      val evaluator   = Evaluator(M)
      val result      = evaluator.eval(graph, ctx, optimize)
      val json = result.flatMap(_.toJson).copoint filterNot { jvalue =>
        (jvalue \ "value") == JUndefined
      }

      var extractIdTime: Long      = 0L
      var jvalueToSValueTime: Long = 0L

      val events = json map { jvalue =>
        (Vector(extractIds(jvalue \ "key"): _*), jvalueToSValue(jvalue \ "value"))
      }

      val back = events.toSet
      evaluator.report.done.copoint
      back
    }
  }

  protected def jvalueToSValue(value: JValue): SValue = value match {
    case JUndefined   => sys.error("don't use jnothing; doh!")
    case JNull        => SNull
    case JBool(value) => SBoolean(value)
    case JNum(bi)     => SDecimal(bi)
    case JString(str) => SString(str)

    case JObject(fields) => {
      val map: Map[String, SValue] = fields.map({
        case JField(name, value) => (name, jvalueToSValue(value))
      })(collection.breakOut)

      SObject(map)
    }

    case JArray(values) =>
      SArray(Vector(values map jvalueToSValue: _*))
  }
}

trait LongIdMemoryDatasetConsumer[M[+ _]] extends MemoryDatasetConsumer[M] {
  type IdType = SValue
  def extractIds(jv: JValue): Seq[SValue] = (jv --> classOf[JArray]).elements map jvalueToSValue
}
