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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.contrib.pathy.ADir
import quasar.contrib.scalaz.catchable._
import quasar.effect._
import quasar.ejson.EJson
import quasar.fp.reflNT
import quasar.fp.free._
import quasar.physical.marklogic._
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._
import quasar.qscript.{Read => _, _}
import quasar.qscript.{MapFuncsCore => MFCore}

import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka._
import com.marklogic.xcc.{ContentSource, Session}
import scalaz._, Scalaz._
import scalaz.concurrent._

final class FilterPlannerSpec extends quasar.Qspec {
  import EitherT.eitherTMonad

  type U      = Fix[Query[Fix[EJson], ?]]
  type SR[A]  = Const[ShiftedRead[ADir], A]
  type QSR[A] = Coproduct[QScriptCore[Fix, ?], SR, A]

  type M[A] = MarkLogicPlanErrT[PrologT[StateT[Free[XccEvalEff, ?], Long, ?], ?], A]

  def filterExpr(adir: ADir, field: String): Fix[QSR] = {
    def eq(lhs: FreeMap[Fix], rhs: String): FreeMap[Fix] =
      Free.roll(MFC(MFCore.Eq(lhs, MFCore.StrLit(rhs))))

    def projectField(str: String): FreeMap[Fix] =
      Free.roll(MFC(MFCore.ProjectField(HoleF, MFCore.StrLit(str))))

    def shiftedRead(path: ADir): Fix[QSR] =
      Fix(Inject[SR, QSR].inj(Const(ShiftedRead(path, IncludeId))))

    def filter(src: Fix[QSR], f: FreeMap[Fix]): Fix[QSR] =
      Fix(Inject[QScriptCore[Fix, ?], QSR].inj(Filter(src, f)))

    filter(shiftedRead(adir), eq(projectField(field), "foobar"))
  }

  def runJson(qs: Fix[QSR]): Free[XccEvalEff, (Prologs, MarkLogicPlannerError \/ (Search[U] \/ XQuery))] = {
    def runJ[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc](qs: Fix[QSR]
    ): F[Search[U] \/ XQuery] =
      qs.cataM(Planner[F, DocType.Json, QSR, Fix[EJson]].plan[U])

    runJ[M](qs).run.run.eval(1)
  }

  def runXcc[A](f: Free[XccEvalEff, A], sess: Session, cs: ContentSource): Task[A] =
    (MonotonicSeq.fromZero map { (monoSeq: MonotonicSeq ~> Task) =>
      val xccToTask: XccEvalEff ~> Task =
        reflNT[Task] :+: monoSeq :+: Read.constant[Task, Session](sess) :+: Read.constant[Task, ContentSource](cs)
      val eval: Free[XccEvalEff, ?] ~> Task = foldMapNT(xccToTask)

      eval(f)
    }).join
}
