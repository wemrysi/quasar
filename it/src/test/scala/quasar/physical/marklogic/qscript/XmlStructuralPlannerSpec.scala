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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.fp.eitherT._
import quasar.physical.marklogic.fmt
import quasar.physical.marklogic.xquery._

import matryoshka._
import scalaz._, Scalaz._

final class XmlStructuralPlannerSpec
  extends StructuralPlannerSpec[XmlStructuralPlannerSpec.XmlPlan, fmt.XML] {

  import XmlStructuralPlannerSpec.XmlPlan

  val toM = λ[XmlPlan ~> M](xp => EitherT(WriterT.writer(xp.leftMap(_.shows.wrapNel).run.run.eval(1))))

  // TODO: projecting multiple child elements with the same name results in an array

/*
    "attributes" >> {
      "returns element attributes as an object" >> {
        val book = element("book".xs)(mkSeq_(
          attribute("author".xs)("Ursula K. LeGuin".xs),
          attribute("published".xs)(1972.xqy),
          "The Farthest Shore".xs
        ))

        eval(ejson.attributes[M] apply book) must resultIn(Data.Obj(
          "author"    -> Data._str("Ursula K. LeGuin"),
          "published" -> Data._str("1972")
        ))
      }

      "returns attributes of an empty element" >> {
        val person = element("person".xs)(attribute("name".xs)("Alice".xs))
        eval(ejson.attributes[M] apply person) must resultIn(Data.Obj(
          "name" -> Data._str("Alice")
        ))
      }

      "returns an empty object when element has no attributes" >> {
        val foo = element("foo".xs)("bar".xs)
        eval(ejson.attributes[M] apply foo) must resultIn(Data.Obj())
      }

      "returns an empty object when empty element has no attributes" >> {
        val baz = element("baz".xs)(emptySeq)
        eval(ejson.attributes[M] apply baz) must resultIn(Data.Obj())
      }
    }

    "many-to-array" >> {
      "passes through empty seq" >> {
        eval(ejson.manyToArray[M] apply expr.emptySeq).toOption must beNone
      }

      "passes through single item" >> {
        eval(ejson.manyToArray[M] apply "foo".xs) must resultIn(Data._str("foo"))
      }

      "returns array for more than one item" >> {
        val three = mkSeq_("foo".xs, "bar".xs, "baz".xs)
        eval(ejson.manyToArray[M] apply three) must resultIn(Data._arr(List(
          Data._str("foo"),
          Data._str("bar"),
          Data._str("baz")
        )))
      }
    }
*/
}

object XmlStructuralPlannerSpec {
  type XmlPlan[A] = MarkLogicPlanErrT[WriterT[State[Long, ?], Prologs, ?], A]
}
