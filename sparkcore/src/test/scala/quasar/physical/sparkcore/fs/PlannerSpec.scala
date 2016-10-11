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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar.console
import quasar.qscript.QScriptHelpers
import quasar.qscript._
import quasar.qscript.ReduceFuncs._
import quasar.qscript.MapFuncs.StrLit
import quasar.contrib.pathy._
import quasar.Data
import quasar.DataCodec
import quasar.qscript._

import org.apache.spark._
import org.apache.spark.rdd._
import pathy.Path._
import scalaz._, Scalaz._, scalaz.concurrent.Task
import pathy.Path._
import matryoshka.{Hole => _, _}
import org.specs2.scalaz.DisjunctionMatchers

class PlannerSpec extends quasar.Qspec with QScriptHelpers with DisjunctionMatchers {

  import Planner.SparkState

  sequential

  "Planner" should {
    "shiftedread" in {

      newSc.map ( sc => {
        val fromFile: (SparkContext, AFile) => Task[RDD[String]] =
          (sc: SparkContext, file: AFile) => Task.delay {
            sc.parallelize(List("""{"name" : "tom", "age" : 28}"""))
          }

        val sr = Planner.shiftedread[Fix]
        val alg: AlgebraM[SparkState, Const[ShiftedRead, ?], RDD[Data]] = sr.plan(fromFile )

        val afile: AFile = rootDir </> dir("Users") </> dir("rabbit") </> file("test.json")

        val state: SparkState[RDD[Data]] = alg(Const(ShiftedRead(afile, IncludeId)))
        state.eval(sc).run.unsafePerformSync must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_== 1
            results(0) must_== Data.Obj(ListMap(
              "name" -> Data.Str("tom"),
              "age" -> Data.Int(28)
            ))
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.map" in {
      newSc.map ( sc => {
        val qscore = Planner.qscriptCore[Fix]
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US")))
        ))

        def func: FreeMap = ProjectFieldR(HoleF, StrLit("country"))
        val map = quasar.qscript.Map(src, func)

        val state: SparkState[RDD[Data]] = alg(map)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_== 3
            results(0) must_== Data.Str("Poland")
            results(1) must_== Data.Str("Poland")
            results(2) must_== Data.Str("US")
        }

        sc.stop
      }).run.unsafePerformSync
      ok
    }


    "core.reduce" in {
      newSc.map ( sc => {
        val qscore = Planner.qscriptCore[Fix]
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US")))
        ))

        def bucket: FreeMap = ProjectFieldR(HoleF, StrLit("country"))
        def reducers: List[ReduceFunc[FreeMap]] = List(Max(ProjectFieldR(HoleF, StrLit("age"))))
        def repair: Free[MapFunc, ReduceIndex] = Free.point(ReduceIndex(0))
        val reduce = Reduce(src, bucket, reducers, repair)

        val state: SparkState[RDD[Data]] = alg(reduce)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_== 2
            results(1) must_== Data.Int(32)
            results(0) must_== Data.Int(23)
        }

        sc.stop

      }).run.unsafePerformSync
      ok
    }
  }

  val emptyFF: (SparkContext, AFile) => Task[RDD[String]] =
    (sc: SparkContext, file: AFile) => Task.delay {
      sc.parallelize(List())
    }

  private def newSc(): OptionT[Task, SparkContext] = for {
    uriStr <- console.readEnv("QUASAR_SPARK_LOCAL")
    uriData <- OptionT(Task.now(DataCodec.parse(uriStr)(DataCodec.Precise).toOption))
    slData <- uriData match {
      case Data.Obj(m) => OptionT(Task.delay(m.get("sparklocal")))
      case _ => OptionT.none[Task, Data]
    }
    uri <- slData match {
      case Data.Obj(m) => OptionT(Task.delay(m.get("connectionUri")))
      case _ => OptionT.none[Task, Data]
    }
    masterAndRoot <- uri match {
      case Data.Str(s) => s.point[OptionT[Task, ?]]
      case _ => OptionT.none[Task, String]
    }
  } yield {
    val master = masterAndRoot.split('|')(0)
    val config = new SparkConf().setMaster(master).setAppName(this.getClass().getName())
    new SparkContext(config)
  }
}
