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

package quasar.qscript

import quasar.Predef._
import quasar.{LogicalPlan => LP, Data, CompilerHelpers}
import quasar.ejson
import quasar.fp._
import quasar.qscript.MapFuncs._
import quasar.std.StdLib
import quasar.std.StdLib._

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "replan" should {
    "convert a constant boolean" in {
       // "select true"
       convert(listContents.some, LP.Constant(Data.Bool(true))) must
       equal(
         QC.inj(Map(RootR, BoolLit(true))).embed.some)
    }

    "fail to convert a constant set" in {
      // "select {\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5}{*} limit 3 offset 1"
      convert(
        listContents.some,
        LP.Constant(Data.Set(List(
          Data.Obj(ListMap("0" -> Data.Int(2))),
          Data.Obj(ListMap("0" -> Data.Int(3))))))) must
      equal(None)
    }

    "convert a simple read" in {
      convert(listContents.some, lpRead("/foo/bar")) must
      equal(
        SP.inj(LeftShift(
          R.inj(Const[Read, Fix[QS]](Read(rootDir </> dir("foo") </> file("bar")))).embed,
          HoleF,
          Free.point(RightSide))).embed.some)
    }

    // FIXME: This can be simplified to a Union of the Reads - the LeftShift
    //        cancels out the MakeMaps.
    "convert a directory read" in {
      convert(listContents.some, lpRead("/foo")) must
      equal(
        SP.inj(LeftShift(
          SP.inj(Union(RootR,
            Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("city"))))), Free.roll(MakeMap(StrLit("city"), HoleF))))),
            Free.roll(SP.inj(Union(Free.roll(DE.inj(Const[DeadEnd, FreeQS[Fix]](Root))),
              Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("zips"))))), Free.roll(MakeMap(StrLit("zips"), HoleF))))),
              Free.roll(SP.inj(Union(Free.roll(DE.inj(Const[DeadEnd, FreeQS[Fix]](Root))),
                Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("car"))))), Free.roll(MakeMap(StrLit("car"), HoleF))))),
                Free.roll(SP.inj(Union(Free.roll(DE.inj(Const[DeadEnd, FreeQS[Fix]](Root))),
                  Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("person"))))), Free.roll(MakeMap(StrLit("person"), HoleF))))),
                  Free.roll(QC.inj(Map(Free.roll(R.inj(Const[Read, FreeQS[Fix]](Read(rootDir </> dir("foo") </> file("bar"))))), Free.roll(MakeMap(StrLit("bar"), HoleF)))))))))))))))).embed,
          HoleF,
          Free.point(RightSide))).embed.some)
    }

    "convert a squashed read" in {
      // "select * from foo"
      convert(listContents.some, identity.Squash(lpRead("/foo/bar"))) must
      equal(
        SP.inj(LeftShift(
          R.inj(Const[Read, Fix[QS]](Read(rootDir </> dir("foo") </> file("bar")))).embed,
          HoleF,
          Free.point(RightSide))).embed.some)
    }

    "convert a simple read with path projects" in {
      convert(listContents.some, lpRead("/some/bar/car")) must
      equal(
        SP.inj(LeftShift(
          R.inj(Const[Read, Fix[QS]](Read(rootDir </> dir("some") </> file("bar")))).embed,
          ProjectFieldR(HoleF, StrLit("car")),
          Free.point(RightSide))).embed.some)
    }

    "convert a basic invoke" in {
      convert(None, math.Add(lpRead("/foo"), lpRead("/bar")).embed) must
      equal(
        TJ.inj(ThetaJoin(
          RootR,
          Free.roll(SP.inj(LeftShift(
            Free.roll(QC.inj(Map(
              Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("foo"))))),
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(Free.point(LeftSide))),
              Free.roll(MakeArray(Free.point(RightSide)))))))),
          Free.roll(SP.inj(LeftShift(
            Free.roll(QC.inj(Map(
              Free.point(SrcHole),
              ProjectFieldR(HoleF, StrLit("bar"))))),
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(Free.point(LeftSide))),
              Free.roll(MakeArray(Free.point(RightSide)))))))),
          BoolLit(true),
          Inner,
          Free.roll(Add(
            Free.roll(ProjectIndex(
              Free.roll(ProjectIndex(Free.point(LeftSide), IntLit(1))),
              IntLit(1))),
            Free.roll(ProjectIndex(
              Free.roll(ProjectIndex(Free.point(RightSide), IntLit(1))),
              IntLit(1))))))).embed.some)
    }

    "convert project object and make object" in {
      convert(
        None,
        identity.Squash(
          makeObj(
            "name" -> structural.ObjectProject(
              lpRead("/city"),
              LP.Constant(Data.Str("name")))))) must
      equal(
        SP.inj(LeftShift(
          RootR,
          ProjectFieldR(HoleF, StrLit("city")),
          Free.roll(MakeMap[Fix, JoinFunc[Fix]](
            StrLit[Fix, JoinSide]("name"),
            ProjectFieldR(
              Free.point[MapFunc[Fix, ?], JoinSide](RightSide),
              StrLit[Fix, JoinSide]("name")))))).embed.some)
    }

    "convert a basic reduction" in {
      convert(
        listContents.some,
        agg.Sum[FLP](lpRead("/person"))) must
      equal(
        QC.inj(Reduce(
          SP.inj(LeftShift(
            R.inj(Const[Read, Fix[QS]](Read(rootDir </> file("person")))).embed,
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(Free.point(LeftSide))),
              Free.roll(MakeArray(Free.point(RightSide))))))).embed,
          Free.roll(MakeArray(Free.roll(MakeMap(StrLit("f"), StrLit("person"))))),
          List(ReduceFuncs.Sum[FreeMap[Fix]](
            Free.roll(ProjectIndex(
              Free.roll(ProjectIndex(HoleF, IntLit(1))),
              IntLit(1))))),
          Free.point(ReduceIndex(0)))).embed.some)
    }

    "convert a basic reduction wrapped in an object" in {
      // "select sum(height) from person"
      convert(
        None,
        makeObj(
          "0" ->
            agg.Sum[FLP](structural.ObjectProject(lpRead("/person"), LP.Constant(Data.Str("height")))))) must
      equal(
        QC.inj(Reduce(
          SP.inj(LeftShift(
            RootR,
            ProjectFieldR(HoleF, StrLit("person")),
            ProjectFieldR(
              Free.point(RightSide),
              StrLit("height")))).embed,
          Free.roll(MakeArray(
            Free.roll(MakeMap(
              StrLit("j"),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.roll(MakeMap(StrLit("f"), StrLit("person"))))),
                Free.roll(MakeArray(NullLit())))))))),
          List(ReduceFuncs.Sum[FreeMap[Fix]](HoleF)),
          Free.roll(MakeMap(StrLit("0"), Free.point(ReduceIndex(0)))))).embed.some)
    }

    "convert a flatten array" in {
      // "select loc[:*] from zips",
      convert(
        None,
        makeObj(
          "loc" ->
            structural.FlattenArray[FLP](
              structural.ObjectProject(lpRead("/zips"), LP.Constant(Data.Str("loc")))))) must
      equal(
        SP.inj(LeftShift(
          SP.inj(LeftShift(
            RootR,
            ProjectFieldR(HoleF, StrLit("zips")),
            ProjectFieldR(
              Free.point(RightSide),
              StrLit("loc")))).embed,
          HoleF,
          Free.roll(MakeMap(StrLit("loc"), Free.point(RightSide))))).embed.some)
    }

    "convert a constant shift array" in {
      // this query never makes it to LP->QS transform because it's a constant value
      // "foo := (1,2,3); select * from foo"
      convert(
        None,
        identity.Squash[FLP](
          structural.ShiftArray[FLP](
            structural.ArrayConcat[FLP](
              structural.ArrayConcat[FLP](
                structural.MakeArrayN[Fix](LP.Constant(Data.Int(1))),
                structural.MakeArrayN[Fix](LP.Constant(Data.Int(2)))),
              structural.MakeArrayN[Fix](LP.Constant(Data.Int(3))))))) must
      equal(
        SP.inj(LeftShift(
          RootR,
          Free.roll(Nullary(
            CommonEJson.inj(ejson.Arr(List(
              ExtEJson.inj(ejson.Int[Fix[ejson.EJson]](1)).embed,
              ExtEJson.inj(ejson.Int[Fix[ejson.EJson]](2)).embed,
              ExtEJson.inj(ejson.Int[Fix[ejson.EJson]](3)).embed))).embed)),
          Free.point(RightSide))).embed.some)
    }.pendingUntilFixed

    "convert a read shift array" in {
      convert(
        None,
        LP.Let('x, lpRead("/foo/bar"),
          structural.ShiftArray[FLP](
            structural.ArrayConcat[FLP](
              structural.ArrayConcat[FLP](
                structural.ObjectProject[FLP](LP.Free('x), LP.Constant(Data.Str("baz"))),
                structural.ObjectProject[FLP](LP.Free('x), LP.Constant(Data.Str("quux")))),
              structural.ObjectProject[FLP](LP.Free('x), LP.Constant(Data.Str("ducks"))))))) must
      equal(RootR.some) // TODO incorrect expectation
    }.pendingUntilFixed

    "convert a shift/unshift array" in {
      // "select [loc[_:] * 10 ...] from zips",
      convert(
        None,
        makeObj(
          "0" ->
            structural.UnshiftArray[FLP](
              math.Multiply[FLP](
                structural.ShiftArrayIndices[FLP](
                  structural.ObjectProject(lpRead("/zips"), LP.Constant(Data.Str("loc")))),
                LP.Constant(Data.Int(10)))))) must
      equal(
        QC.inj(Reduce(
          SP.inj(LeftShift(
            RootR,
            Free.roll(DupArrayIndices(
              ProjectFieldR(
                ProjectFieldR(HoleF, StrLit("zips")),
                StrLit("loc")))),
            Free.roll(Multiply(Free.point(RightSide), IntLit(10))))).embed,
          HoleF,
          List(ReduceFuncs.UnshiftArray(HoleF[Fix])),
          Free.roll(MakeMap[Fix, Free[MapFunc[Fix, ?], ReduceIndex]](
            StrLit[Fix, ReduceIndex]("0"),
            Free.point(ReduceIndex(0)))))).embed.some)
    }.pendingUntilFixed

    "convert a filter" in {
      // "select * from foo where bar between 1 and 10"
      convert(
        listContents.some,
        StdLib.set.Filter[FLP](
          lpRead("/bar"),
          relations.Between[FLP](
            structural.ObjectProject(lpRead("/bar"), LP.Constant(Data.Str("baz"))),
            LP.Constant(Data.Int(1)),
            LP.Constant(Data.Int(10))))) must
      equal(
        QC.inj(Filter(
          SP.inj(LeftShift(
            R.inj(Const[Read, Fix[QS]](Read(rootDir </> file("bar")))).embed,
            HoleF,
            Free.point(RightSide))).embed,
          Free.roll(Between(
            ProjectFieldR(HoleF, StrLit("baz")),
            IntLit(1),
            IntLit(10))))).embed.some)
    }.pendingUntilFixed

    // an example of how logical plan expects magical "left" and "right" fields to exist
    "convert magical query" in {
      // "select * from person, car",
      convert(
        None,
        LP.Let('__tmp0,
          StdLib.set.InnerJoin(lpRead("/person"), lpRead("/car"), LP.Constant(Data.Bool(true))),
          identity.Squash[FLP](
            structural.ObjectConcat[FLP](
              structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("left"))),
              structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("right"))))))) must
      equal(RootR.some) // TODO incorrect expectation
    }.pendingUntilFixed

    "convert basic join with explicit join condition" in {
      //"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",

      val lp = LP.Let('__tmp0, lpRead("/foo"),
        LP.Let('__tmp1, lpRead("/bar"),
          LP.Let('__tmp2,
            StdLib.set.InnerJoin[FLP](LP.Free('__tmp0), LP.Free('__tmp1),
              relations.Eq[FLP](
                structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("id"))),
                structural.ObjectProject(LP.Free('__tmp1), LP.Constant(Data.Str("foo_id"))))),
            makeObj(
              "name" ->
                structural.ObjectProject[FLP](
                  structural.ObjectProject(LP.Free('__tmp2), LP.Constant(Data.Str("left"))),
                  LP.Constant(Data.Str("name"))),
              "address" ->
                structural.ObjectProject[FLP](
                  structural.ObjectProject(LP.Free('__tmp2), LP.Constant(Data.Str("right"))),
                  LP.Constant(Data.Str("address")))))))
      convert(None, lp) must equal(
        QC.inj(Map(RootR, ProjectFieldR(HoleF, StrLit("foo")))).embed.some)
    }.pendingUntilFixed
  }
}
