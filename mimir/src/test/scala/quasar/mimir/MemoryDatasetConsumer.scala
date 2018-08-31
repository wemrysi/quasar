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

package quasar.mimir

import quasar.blueeyes._, json._
import quasar.yggdrasil.execution.EvaluationContext

import scalaz.Validation

trait MemoryDatasetConsumer extends EvaluatorModule {
  type X = Throwable

  type IdType = JValue
  type SEvent = (Vector[IdType], JValue)

  def Evaluator: EvaluatorLike

  def extractIds(jv: JValue): Seq[IdType]

  def consumeEval(graph: DepGraph, ctx: EvaluationContext, optimize: Boolean = true)
      : Validation[X, Set[SEvent]] = {
    Validation.fromTryCatchNonFatal {
      val evaluator = Evaluator
      val result = evaluator.eval(graph, ctx, optimize)
      val json = result.flatMap(_.toJson).unsafeRunSync filterNot { rvalue =>
        (rvalue.toJValue \ "value") == JUndefined
      }

      val events: Iterable[SEvent] = json map { rvalue =>
        (Vector(extractIds(rvalue.toJValue \ "key"): _*), rvalue.toJValue \ "value")
      }

      val back = events.toSet
      evaluator.report.done.unsafeRunSync
      back
    }
  }
}

trait LongIdMemoryDatasetConsumer extends MemoryDatasetConsumer {
  def extractIds(jv: JValue): Seq[JValue] = (jv --> classOf[JArray]).elements
}
