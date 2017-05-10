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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.javascript._
import quasar.jscore, jscore.JsFn
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._
import quasar.specs2.QuasarMatchers._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

class WorkflowBuilderSpec extends quasar.Qspec {
  import WorkflowBuilder._
  import CollectionUtil._

  val builder = WorkflowBuilder.Ops[WorkflowF]
  import builder._
  import fixExprOp._

  val readZips = read(collection("db", "zips"))

  implicit val workflowEqual: Equal[Fix[WorkflowF]] = Equal.equalA

  "WorkflowBuilder" should {

    "make simple read" in {
      val op = build(read(collection("db", "zips"))).evalZero

      op must beRightDisjunction($read[WorkflowF](collection("db", "zips")))
    }

    "group constant in proj" in {
      val read = builder.read(collection("db", "zips"))
      val one = ExprBuilder(read, \&/-($literal(Bson.Int32(1))))
      val grouped = groupBy(one, List(one))
      val obj = makeObject(reduce(grouped)($sum(_)), "total")
      val op = build(obj).evalZero

      op must beRightDisjOrDiff(
        chain[Workflow]($read(collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> $sum($literal(Bson.Int32(1))))),
            \/-($literal(Bson.Null)))))
    }

    "normalize" should {
      val readFoo = CollectionBuilder($read[WorkflowF](collection("db", "foo")), Root(), None)

      "collapse simple reference to JS" in {
        val w = DocBuilder[WorkflowF](
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> -\&/(jscore.JsFn(jscore.Name("x"), jscore.Literal(Js.Bool(true)))))),
          ListMap(
            BsonField.Name("0") -> \&/-($var(DocField(BsonField.Name("__tmp"))))))
        val exp = DocBuilder[WorkflowF](
          readFoo,
          ListMap(
            BsonField.Name("0") -> -\&/(jscore.JsFn(jscore.Name("y"), jscore.Literal(Js.Bool(true))))))

        normalize[WorkflowF].apply(w.project) must_== exp.project
      }

      "collapse reference in ExprOp" in {
        val w = DocBuilder[WorkflowF](
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> \&/-($var(DocField(BsonField.Name("foo")))))),
          ListMap(
            BsonField.Name("0") -> \&/-($toLower($var(DocField(BsonField.Name("__tmp")))))))
        val exp = DocBuilder[WorkflowF](
          readFoo,
          ListMap(
            BsonField.Name("0") -> \&/-($toLower($var(DocField(BsonField.Name("foo")))))))

        normalize[WorkflowF].apply(w.project) must_== exp.project
      }

      "collapse reference to JS in ExprOp" in {
        val w = DocBuilder[WorkflowF](
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> -\&/(jscore.JsFn(jscore.Name("x"), jscore.Literal(Js.Str("ABC")))))),
          ListMap(
            BsonField.Name("0") -> \&/.Both(
              jscore.JsFn(jscore.Name("x"), jscore.Call(jscore.Select(jscore.Select(jscore.ident("x"), "__tmp"), "toLowerCase"), Nil)),
              $toLower($var(DocField(BsonField.Name("__tmp")))))))
        val exp = DocBuilder[WorkflowF](
          readFoo,
          ListMap(
            BsonField.Name("0") -> -\&/(jscore.JsFn(jscore.Name("x"),
              jscore.Call(
                jscore.Select(jscore.Literal(Js.Str("ABC")), "toLowerCase"),
                Nil)))))

        normalize[WorkflowF].apply(w.project) must_== exp.project
      }

      "collapse reference through $$ROOT" in {
        val w = DocBuilder[WorkflowF](
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> \&/-($$ROOT))),
          ListMap(
            BsonField.Name("foo") -> \&/-($var(DocField(BsonField.Name("__tmp") \ BsonField.Name("foo"))))))
        val exp = DocBuilder[WorkflowF](
          readFoo,
          ListMap(
            BsonField.Name("foo") -> \&/(
              JsFn(JsFn.defaultName, jscore.Select(jscore.Ident(JsFn.defaultName), "foo")),
              $var(DocField(BsonField.Name("foo"))))))

        normalize[WorkflowF].apply(w.project) must_== exp.project
      }

      "collapse JS reference" in {
        val w = DocBuilder[WorkflowF](
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> \&/.Both(
                JsFn(JsFn.defaultName, jscore.Select(jscore.Ident(JsFn.defaultName), "foo")),
                $var(DocField(BsonField.Name("foo")))))),
          ListMap(
            BsonField.Name("0") -> -\&/(jscore.JsFn(jscore.Name("x"),
              jscore.Select(jscore.Select(jscore.ident("x"), "__tmp"), "length")))))

        val exp = DocBuilder[WorkflowF](
          readFoo,
          ListMap(
            BsonField.Name("0") -> -\&/(jscore.JsFn(jscore.Name("y"),
              jscore.Select(jscore.Select(jscore.ident("y"), "foo"), "length")))))

        normalize[WorkflowF].apply(w.project) must_== exp.project
      }

      "collapse expression that contains a projection" in {
        val w = DocBuilder[WorkflowF](
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp0") -> \&/.Both(
                JsFn(JsFn.defaultName,
                  jscore.BinOp(jscore.Sub,
                    jscore.Select(jscore.Ident(JsFn.defaultName), "pop"),
                    jscore.Literal(Js.Num(1, false)))),
                $subtract($var(DocField(BsonField.Name("pop"))), $literal(Bson.Int64(1)))))),

          ListMap(
            BsonField.Name("__tmp3") -> -\&/(jscore.JsFn(jscore.Name("x"),
              jscore.Arr(List(jscore.Select(jscore.ident("x"), "__tmp0")))))))

        val exp = DocBuilder[WorkflowF](
          readFoo,
          ListMap(
            BsonField.Name("__tmp3") -> -\&/(jscore.JsFn(jscore.Name("x"),
              jscore.Arr(List(jscore.BinOp(jscore.Sub,
                jscore.Select(jscore.ident("x"), "pop"),
                jscore.Literal(Js.Num(1, false)))))))))

        normalize[WorkflowF].apply(w.project) must_== exp.project
      }

      "collapse this" in {
        val w =
          DocBuilder[WorkflowF](
            DocBuilder(
              DocBuilder(
                readFoo,
                ListMap(
                  BsonField.Name("__tmp4") ->
                    \&/-($and($lt($literal(Bson.Null), $field("pop")), $lt($field("pop"), $literal(Bson.Text(""))))),
                  BsonField.Name("__tmp5") -> \&/-($$ROOT))),
              ListMap(
                BsonField.Name("__tmp6") ->
                  \&/-($cond($field("__tmp4"), $field("__tmp5", "pop"), $literal(Bson.Null))),
                BsonField.Name("__tmp7") -> \&/-($field("__tmp5")))),
            ListMap(
              BsonField.Name("__tmp8") -> \&/-($field("__tmp7", "city")),
              BsonField.Name("__tmp9") -> \&/-($field("__tmp6"))))

        val exp = DocBuilder[WorkflowF](
          readFoo,
          ListMap(
            BsonField.Name("__tmp8") -> \&/(
              JsFn(JsFn.defaultName, jscore.Select(jscore.Ident(JsFn.defaultName), "city")),
              $field("city")),
            BsonField.Name("__tmp9") ->
              \&/-($cond($and($lt($literal(Bson.Null), $field("pop")), $lt($field("pop"), $literal(Bson.Text("")))), $field("pop"), $literal(Bson.Null)))))

        normalize[WorkflowF].apply(w.project) must_== exp.project
      }
    }
  }
}
