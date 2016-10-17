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

package ygg.tests

import scala.Predef.$conforms
import ygg._, common._, json._, table._

object DummyModule extends BlockStoreTestModule {
  val projections = Map[Path, Projection]()
}
class BlockStoreLoadTestModule(sampleData: SampleData) extends BlockStoreTestModule {
  val Some((idCount, schema)) = sampleData.schema
  val actualSchema            = CValueGenerators.inferSchema(sampleData.data map { _ \ "value" })

  val projections = List(actualSchema).map { subschema =>
    val stream = sampleData.data flatMap { jv =>
      val back = subschema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
        case (obj, (jpath, ctype)) => {
          val vpath       = JPath(JPathField("value") :: jpath.nodes)
          val valueAtPath = jv.get(vpath)

          if (compliesWithSchema(valueAtPath, ctype)) {
            obj.set(vpath, valueAtPath)
          } else {
            obj
          }
        }
      }

      if (back \ "value" == JUndefined)
        None
      else
        Some(back)
    }

    Path("/test") -> Projection(stream)
  } toMap
}

trait BlockStoreTestModule extends BlockTableModule with ColumnarTableModuleTestSupport
