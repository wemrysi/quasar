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

package quasar.qscript.qsu

import slamdata.Predef.{List, Nil}

import quasar.{Qspec, TreeMatchers}
import quasar.Planner.PlannerError
import quasar.ejson.{EJson, Fixed}
import quasar.fp.coproductEqual
import quasar.qscript.{construction, ExcludeId, Hole, ReduceFuncs}
import quasar.qscript.qsu.QScriptUniform.Rotation

import matryoshka.delayEqual
import matryoshka.data.Fix
import matryoshka.data.free._
import scalaz.{\/, \/-}

object ReifyBucketsSpec extends Qspec with QSUTTypes[Fix] with TreeMatchers {
  import QSUGraph.Extractors._
  import QScriptUniform.DTrans

  val J = Fixed[Fix[EJson]]
  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]

  def reifyBuckets(qsu: Fix[QScriptUniform]): PlannerError \/ QSUGraph =
    ApplyProvenance[Fix, PlannerError \/ ?](QSUGraph.fromTree(qsu))
      .flatMap(ReifyBuckets[Fix, PlannerError \/ ?])
      .map(_.graph)

  "reifying buckets" should {
    "extract mappable region up to bucket source and use in reducer" >> {
      // select sum(pop) from zips group by state
      val tree =
        qsu.lpReduce(
          qsu.map(
            qsu.dimEdit(
              qsu.tread1("zips"),
              DTrans.Group(
                func.ProjectKeyS(func.Hole, "state"))),
            func.ProjectKeyS(func.Hole, "pop")),
          ReduceFuncs.Sum(()))

      val expBucket =
        func.ProjectKeyS(func.Hole, "state") map (Access.value(_))

      val expReducer =
        func.ProjectKeyS(func.Hole, "pop")

      reifyBuckets(tree) must beLike {
        case \/-(QSReduce(
            LeftShift(Read(_), _, ExcludeId, _, Rotation.ShiftMap),
            List(b),
            List(ReduceFuncs.Sum(r)),
            _)) =>

          (b must beTreeEqual(expBucket)) and
          (r must beTreeEqual(expReducer))
      }
    }

    "leave source untouched when no buckets" >> {
      // select sum(pop) from zips
      val tree =
        qsu.lpReduce(
          qsu.map(
            qsu.tread1("zips"),
            func.ProjectKeyS(func.Hole, "pop")),
          ReduceFuncs.Sum(()))

      val expFm =
        func.ProjectKeyS(func.Hole, "pop")

      reifyBuckets(tree) must beLike {
        case \/-(
          QSReduce(
            Map(LeftShift(Read(_), _, ExcludeId, _, Rotation.ShiftMap), fm),
            Nil,
            List(ReduceFuncs.Sum(r)),
            _)) =>

          (fm must beTreeEqual(expFm)) and
          (r must beTreeEqual(func.Hole))
      }
    }

    "leave source untouched when constant buckets" >> {
      // select sum(pop) from zips group by 7
      val tree =
        qsu.lpReduce(
          qsu.map(
            qsu.dimEdit(
              qsu.tread1("zips"),
              DTrans.Group(func.Constant(J.int(7)))),
            func.ProjectKeyS(func.Hole, "pop")),
          ReduceFuncs.Sum(()))

      val expFm =
        func.ProjectKeyS(func.Hole, "pop")

      val expB =
        func.Constant[Access[Hole]](J.int(7))

      reifyBuckets(tree) must beLike {
        case \/-(
          QSReduce(
            Map(LeftShift(Read(_), _, ExcludeId, _, Rotation.ShiftMap), fm),
            List(b),
            List(ReduceFuncs.Sum(r)),
            _)) =>

          (fm must beTreeEqual(expFm)) and
          (b must beTreeEqual(expB)) and
          (r must beTreeEqual(func.Hole))
      }
    }

    // TODO: When does a bucketed sort arise?
    "reify sort buckets" >> todo
  }
}
