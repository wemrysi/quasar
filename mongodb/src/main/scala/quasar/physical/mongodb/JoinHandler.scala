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

// TOOD: move to .planner package
package quasar.physical.mongodb

import quasar.Predef._
import quasar.fp._
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.std.StdLib._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import Workflow._
import WorkflowBuilder._

import matryoshka._
import scalaz._, Scalaz._

final case class JoinHandler[F[_], G[_]](run: (JoinType, JoinSource[F], JoinSource[F]) => G[WorkflowBuilder[F]]) {

  def apply(tpe: JoinType, left: JoinSource[F], right: JoinSource[F]): G[WorkflowBuilder[F]] =
    run(tpe, left, right)
}

final case class JoinSource[F[_]](src: WorkflowBuilder[F], keys: List[WorkflowBuilder[F]], js: Option[List[JsFn]])

object JoinHandler {

  // FIXME: these have to match the names used in the logical plan. Should
  //        change this to ensure left0/right0 are `Free` and pull the names
  //        from those.
  val LeftName: BsonField.Name = BsonField.Name("left")
  val RightName: BsonField.Name = BsonField.Name("right")

  /** When possible, plan a join using the more efficient \$lookup operator in
    * the aggregation pipeline.
    */
  def pipeline[F[_]: Functor: Coalesce: Crush: Crystallize]
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: WorkflowOp3_2F :<: F, ev2: Show[WorkflowBuilder[F]])
    : JoinHandler[F, OptionT[WorkflowBuilder.M, ?]] = JoinHandler({ (tpe, left, right) =>

    val WB = WorkflowBuilder.Ops[F]

    def lookup(
      lSrc: WorkflowBuilder[F], lField: BsonField, lName: BsonField.Name,
      rColl: Collection, rField: BsonField, rName: BsonField.Name) =
    {
      val left = WB.makeObject(lSrc, lName.asText)
      val filtered = WB.filter(left,
        List(ExprBuilder(left, \/-($var(DocField(lName \ lField))))),
        { case List(f) => Selector.Doc(f -> Selector.Neq(Bson.Null)) })
      workflow(filtered).map { case (left, _) =>
        CollectionBuilder(
          chain[Fix[F]](
            left,
            $lookup(rColl, lName \ lField, rField, rName),
            $unwind(DocField(rName))),
          Root(),
          None)
        }
    }

    (tpe, left, right) match {
      case (set.InnerJoin, IsLookupInput(src, expr), IsLookupFrom(coll, field)) =>
        lookup(src, expr, LeftName, coll, field, RightName).liftM[OptionT]

      case (set.InnerJoin, IsLookupFrom(coll, field), IsLookupInput(src, expr)) =>
        lookup(src, expr, RightName, coll, field, LeftName).liftM[OptionT]

      case _ => OptionT.none
    }
  })

  object IsLookupInput {
    /** Matches a source which is suitable to be the "left" side of a
      * \$lookup-based join; essentially, it needs to have a single key.
      */
    def unapply[F[_]](arg: JoinSource[F])(implicit ev: Equal[Fix[WorkflowBuilderF[F, ?]]])
      : Option[(WorkflowBuilder[F], BsonField)] =
      arg.keys match {
        // TODO: generalize to handle more sources (notably ShapePreservingBuilders)
        case List(Fix(ExprBuilderF(src, \/-($var(DocVar(_, Some(field))))))) if src ≟ arg.src =>
          (src, field).some
        case _ =>
          None
      }
  }
  object IsLookupFrom {
    /** Matches a source which is suitable to be the "right" side of a
      * \$lookup-based join; this has to be a \$project of a single field from
      * a raw \$read.
      */
    def unapply[F[_]](arg: JoinSource[F])
      (implicit ev0: WorkflowOpCoreF :<: F, ev1: Equal[Fix[WorkflowBuilderF[F, ?]]])
      : Option[(Collection, BsonField)] =
      arg match {
        case JoinSource(
              Fix(CollectionBuilderF(Fix($read(coll)), Root(), None)),
              List(Fix(ExprBuilderF(src, \/-($var(DocVar(_, Some(field))))))),
              _) if src ≟ arg.src =>
          (coll, field).some
        case _ =>
          None
      }
  }

  /** Plan an arbitrary join using only "core" operators, which always means a map-reduce. */
  def mapReduce[F[_]: Functor: Coalesce: Crush: Crystallize]
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
    : JoinHandler[F, WorkflowBuilder.M] = JoinHandler({ (tpe, left0, right0) =>

    val ops = Ops[F]
    import ops._

    val leftField0 = LeftName
    val rightField0 = RightName

    def keyMap(keyExpr: List[JsFn], rootField: BsonField.Name, otherField: BsonField.Name): Js.AnonFunDecl =
      $MapF.mapKeyVal(("key", "value"),
        keyExpr match {
          case Nil => Js.Null
          case _   =>
            jscore.Obj(keyExpr.map(_(jscore.ident("value"))).zipWithIndex.foldLeft[ListMap[jscore.Name, JsCore]](ListMap[jscore.Name, JsCore]()) {
              case (acc, (j, i)) => acc + (jscore.Name(i.toString) -> j)
            }).toJs
        },
        Js.AnonObjDecl(List(
          (otherField.asText, Js.AnonElem(Nil)),
          (rootField.asText, Js.AnonElem(List(Js.Ident("value")))))))

    def jsReduce(src: WorkflowBuilder[F], key: List[JsFn],
                 rootField: BsonField.Name, otherField: BsonField.Name):
        (WorkflowBuilder[F], FixOp[F]) =
      (src, $map[F](keyMap(key, rootField, otherField), ListMap()))

    def wbReduce(src: WorkflowBuilder[F], key: List[WorkflowBuilder[F]],
                 rootField: BsonField.Name, otherField: BsonField.Name):
        (WorkflowBuilder[F], FixOp[F]) =
      (DocBuilder(
        reduce(groupBy(src, key))($push(_)),
        ListMap(
          rootField             -> \/-($$ROOT),
          otherField            -> \/-($literal(Bson.Arr(Nil))),
          BsonField.Name("_id") -> \/-($include()))),
        ι)

    val (left, right, leftField, rightField) =
      (left0.js, right0.js) match {
        case (Some(js), _)
            if preferMapReduce(left0.src) && !preferMapReduce(right0.src) =>
          (wbReduce(right0.src, right0.keys, rightField0, leftField0),
            jsReduce(left0.src, js, leftField0, rightField0),
            rightField0, leftField0)
        case (_, Some(js)) =>
          (wbReduce(left0.src, left0.keys, leftField0, rightField0),
            jsReduce(right0.src, js, rightField0, leftField0),
            leftField0, rightField0)
        case (Some(js), _) =>
          (wbReduce(right0.src, right0.keys, rightField0, leftField0),
            jsReduce(left0.src, js, leftField0, rightField0),
            rightField0, leftField0)
        case (None, None) =>
          (wbReduce(left0.src, left0.keys, leftField0, rightField0),
            wbReduce(right0.src, right0.keys, rightField0, leftField0),
            leftField0, rightField0)
      }

    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def padEmpty(side: BsonField): Expression =
      $cond($eq($size($var(DocField(side))), $literal(Bson.Int32(0))),
        $literal(Bson.Arr(List(Bson.Doc(ListMap())))),
        $var(DocField(side)))

    def buildProjection(l: Expression, r: Expression): FixOp[F] =
      $project[F](Reshape(ListMap(leftField -> \/-(l), rightField -> \/-(r)))).apply(_)

    // TODO exhaustive pattern match
    def buildJoin(src: Fix[F], tpe: JoinType): Fix[F] =
      tpe match {
        case set.FullOuterJoin =>
          chain(src,
            buildProjection(padEmpty(leftField), padEmpty(rightField)))
        case set.LeftOuterJoin =>
          chain(src,
            $match[F](Selector.Doc(ListMap(
              leftField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection($var(DocField(leftField)), padEmpty(rightField)))
        case set.RightOuterJoin =>
          chain(src,
            $match[F](Selector.Doc(ListMap(
              rightField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(padEmpty(leftField), $var(DocField(rightField))))
        case set.InnerJoin =>
          chain(
            src,
            $match[F](
              Selector.Doc(ListMap(
                leftField.asInstanceOf[BsonField] -> nonEmpty,
                rightField -> nonEmpty))))
        case _ => scala.sys.error("How did this get here?")
      }

    val rightReduce = {
      import Js._

      AnonFunDecl(List("key", "values"),
        List(
          VarDef(List(("result",
            AnonObjDecl(List(
              (leftField.asText, AnonElem(Nil)),
              (rightField.asText, AnonElem(Nil))))))),
          Call(Select(Ident("values"), "forEach"),
            List(AnonFunDecl(List("value"),
              // TODO: replace concat here with a more efficient operation
              //      (push or unshift)
              List(
                BinOp("=",
                  Select(Ident("result"), leftField.asText),
                  Call(Select(Select(Ident("result"), leftField.asText), "concat"),
                    List(Select(Ident("value"), leftField.asText)))),
                BinOp("=",
                  Select(Ident("result"), rightField.asText),
                  Call(Select(Select(Ident("result"), rightField.asText), "concat"),
                    List(Select(Ident("value"), rightField.asText)))))))),
          Return(Ident("result"))))
    }

    (workflow(left._1) |@| workflow(right._1)) { case ((l, _), (r, _)) =>
      CollectionBuilder(
        chain(
          $foldLeft[F](
            left._2(l),
            chain(r, right._2, $reduce[F](rightReduce, ListMap()))),
          (op: Fix[F]) => buildJoin(op, tpe),
          $unwind[F](DocField(leftField)),
          $unwind[F](DocField(rightField))),
        Root(),
        None)
    }
  })

  // TODO: This is an approximation. If we could postpone this decision until
  //      `Workflow.crush`, when we actually have a task (whether aggregation or
  //       mapReduce) in hand, we would know for sure.
  private def preferMapReduce[F[_]: Coalesce: Crush: Crystallize: Functor](wb: WorkflowBuilder[F])
    (implicit ev0: WorkflowOpCoreF :<: F, ev1: Show[WorkflowBuilder[F]])
    : Boolean = {
    // TODO: Get rid of this when we functorize WorkflowTask
    def checkTask(wt: workflowtask.WorkflowTask): Boolean = wt match {
      case workflowtask.FoldLeftTask(_, _)     => true
      case workflowtask.MapReduceTask(_, _, _) => true
      case workflowtask.PipelineTask(src, _)   => checkTask(src)
      case _                                   => false
    }

    workflow[F](wb).evalZero.fold(
      κ(false),
      wf => checkTask(task(Crystallize[F].crystallize(wf._1))))
  }
}
