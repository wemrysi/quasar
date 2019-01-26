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

package quasar.impl.evaluate

import slamdata.Predef._

import cats.effect.Sync
import cats.effect.concurrent.Ref

import quasar.{ParseInstruction, ParseType}
import quasar.IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.ParseInstruction.{Cartesian, Ids, Mask, Pivot, Project, Wrap}
import quasar.common.{CPathField, CPathIndex, CPathNode}
import quasar.common.data._

import scala.collection.Iterator

import scalaz.{Functor, Scalaz}, Scalaz._

import shims._

object RValueParseInstructionInterpreter {

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def apply[F[_]: Sync](instructions: List[ParseInstruction])
      : F[RValue => F[List[RValue]]] =
    Ref[F].of(0L).map(interpret(instructions, _))

  private def interpret[F[_]: Sync](instructions: List[ParseInstruction], counter: Ref[F, Long])(rvalue: RValue)
      : F[List[RValue]] =
    instructions.foldLeftM(List[RValue](rvalue)) {
      case (prev, instr @ Mask(_)) =>
        prev.flatMap(interpretMask(instr, _).toList).point[F]

      case (prev, instr @ Pivot(_, _, _)) =>
        prev.flatMap(interpretPivot(instr, _)).point[F]

      case (prev, instr @ Wrap(_, _)) =>
        prev.flatMap(interpretWrap(instr, _) :: Nil).point[F]

      case (prev, instr @ Project(_)) =>
        prev.flatMap(interpretProject(instr, _)).point[F]

      case (prev, instr @ Cartesian(_)) =>
        prev.traverseM(interpretCartesian(instr, counter, _))

      case (prev, Ids) =>
        prev.traverse(interpretIds(counter, _))
    }

  def interpretIds[F[_]: Functor](ref: Ref[F, Long], rvalue: RValue): F[RValue] =
    ref.modify(i => (i + 1, i)) map { id =>
      RValue.rArray(List(RValue.rLong(id), rvalue))
    }

  private def maskTarget(tpes: Set[ParseType], rvalue: RValue): Boolean =
    rvalue match {
      case RMeta(_, _) => tpes.contains(ParseType.Meta)

      case RObject(_) => tpes.contains(ParseType.Object)
      case CEmptyObject => tpes.contains(ParseType.Object)

      case RArray(_) => tpes.contains(ParseType.Array)
      case CEmptyArray => tpes.contains(ParseType.Array)

      case CString(_) => tpes.contains(ParseType.String)
      case CBoolean(_) => tpes.contains(ParseType.Boolean)
      case CNull => tpes.contains(ParseType.Null)

      case CLong(_) => tpes.contains(ParseType.Number)
      case CDouble(_) => tpes.contains(ParseType.Number)
      case CNum(_) => tpes.contains(ParseType.Number)

      // These are encoded as objects from a parser-perspective
      case CLocalDateTime(_) => tpes.contains(ParseType.Object)
      case CLocalDate(_) => tpes.contains(ParseType.Object)
      case CLocalTime(_) => tpes.contains(ParseType.Object)
      case COffsetDateTime(_) => tpes.contains(ParseType.Object)
      case COffsetDate(_) => tpes.contains(ParseType.Object)
      case COffsetTime(_) => tpes.contains(ParseType.Object)
      case CInterval(_) => tpes.contains(ParseType.Object)

      case _ => false
    }

  def interpretMask(mask: Mask, rvalue: RValue): Option[RValue] = {

    // recurse down:
    // remove structure which is definitely not needed based on the current cpaths
    //
    // build up:
    // only retain if type checks
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner(masks: Map[List[CPathNode], Set[ParseType]], rvalue: RValue): Option[RValue] = {

      val prefixes: Set[CPathNode] = masks.keySet collect {
        case head :: _ => head
      }

      def relevantMasks(prefix: CPathNode): Map[List[CPathNode], Set[ParseType]] =
        masks collect {
          case (`prefix` :: tail, tpes) => (tail, tpes)
        }

      if (masks.isEmpty) {
        None
      } else {
        (rvalue, masks.get(Nil)) match {
          case (rv, Some(tpes)) =>
            if (maskTarget(tpes, rv))
              Some(rv) // stop because we subsume
            else
              inner(masks - Nil, rvalue) // continue with the disjunction

          case (RObject(fields), None) =>
            val result: Map[String, Option[RValue]] = fields map {
              case (field, rv) =>
                val cpath = CPathField(field)

                if (prefixes.contains(cpath))
                  (field, inner(relevantMasks(cpath), rv))
                else
                  (field, None)
            }
            val back: Map[String, RValue] = result collect {
              case (field, Some(x)) => (field, x)
            }
            if (back.isEmpty)
              None
            else
              Some(RObject(back))

          case (RArray(elems), None) =>
            val result: List[Option[RValue]] = elems.zipWithIndex map {
              case (elem, idx) =>
                if (prefixes.contains(CPathIndex(idx)))
                  inner(relevantMasks(CPathIndex(idx)), elem)
                else
                  None
            }
            val back: List[RValue] = result collect {
              case (Some(x)) => x
            }
            if (back.isEmpty)
              None
            else
              Some(RArray(back))

          case (_, None) => None
        }
      }
    }

    val input = mask.masks map {
      case (k, v) => (k.nodes, v)
    }

    inner(input, rvalue)
  }

  def interpretProject(project: Project, rvalue: RValue): List[RValue] =
    project.path.nodes.foldLeftM(rvalue) {
      case (rv, CPathField(name)) => RValue.rField1(name).getOption(rv)
      case (rv, CPathIndex(idx)) => RValue.rElement(idx).getOption(rv)
      case _ => None
    }.toList

  def interpretCartesian[F[_]: Sync](cartesian: Cartesian, counter: Ref[F, Long], rvalue: RValue)
      : F[List[RValue]] =
    rvalue match {
      case RObject(fields) =>
        // the components of the cartesian
        val componentsF: F[Map[String, List[RValue]]] =
          cartesian.cartouches.toList.foldLeft(Map[String, List[RValue]]().point[F]) {
            case (accF, (wrap, (deref, instrs))) =>
              for {
                acc <- accF
                res <- fields.get(deref.name).map(interpret[F](instrs, counter)).sequence
              } yield {
                res match {
                  case Some(v) => acc + (wrap.name -> v)
                  case None => acc
                }
              }
          }

        // the cartesian
        componentsF map { components =>
          val res = components.toList.foldLeft(List[Map[String, RValue]]()) {
            case (acc, (name, rvs)) =>
              if (rvs.isEmpty)
                acc
              else if (acc.isEmpty)
                rvs.map(rv => Map(name -> rv))
              else
                rvs.flatMap(rv => acc.map(_.updated(name, rv)))
          }
          res.map(RObject(_))
        }

      case _ => List[RValue]().point[F]
    }

  def interpretPivot(pivot: Pivot, rvalue: RValue): List[RValue] = {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner(remaining: List[CPathNode], rvalue: RValue): List[RValue] =
      remaining match {
        // perform the pivot
        case Nil => {
          (rvalue, pivot.structure) match {

            // pivot object
            case (RObject(fields), ParseType.Object) => {
              val pivoted: List[RValue] = pivot.status match {
                case IdOnly =>
                  fields.foldLeft(List[RValue]()) {
                    case (acc, (k, _)) => CString(k) :: acc
                  }
                case IncludeId => // the qscript expects the results to be returned in an array
                  fields.foldLeft(List[RValue]()) {
                    case (acc, (k, v)) => RArray(CString(k), v) :: acc
                  }
                case ExcludeId =>
                  fields.foldLeft(List[RValue]()) {
                    case (acc, (_, v)) => v :: acc
                  }
              }
              pivoted.reverse
            }

            // pivot array
            case (RArray(elems), ParseType.Array) =>
              pivot.status match {
                case IdOnly =>
                  elems.iterator.zipWithIndex.map(t => CLong(t._2.toLong)).toList
                case IncludeId => // the qscript expects the results to be returned in an array
                  val shifted: Iterator[RValue] = elems.iterator.zipWithIndex map {
                    case (elem, idx) => RArray(CLong(idx.toLong), elem)
                  }
                  shifted.toList
                case ExcludeId => elems
              }

            // pivot empty object drops the row
            case (CEmptyObject, ParseType.Object) => Nil

            // pivot empty array drops the row
            case (CEmptyArray, ParseType.Array) => Nil

            case (v, t) =>
              scala.sys.error(s"No surrounding structure allowed when pivoting. Received: ${(v, t)}")
          }
        }

        // recurse on an object deref
        case CPathField(field) :: tail => rvalue match {
          case obj @ RObject(fields) =>
            fields.toList match {
              case (`field`, target) :: Nil =>
                inner(tail, target).map(v => RObject((field, v)))
              case _ =>
                scala.sys.error(s"No surrounding structure allowed when pivoting. Received: $obj")
            }
          case rv =>
            scala.sys.error(s"No surrounding structure allowed when pivoting. Received: $rv")
        }

        // recurse on an array deref
        case CPathIndex(0) :: tail => rvalue match {
          case arr @ RArray(target :: Nil) =>
            inner(tail, target).map(v => RArray(List(v)))
          case rv =>
            scala.sys.error(s"No surrounding structure allowed when pivoting. Received: $rv")
        }

        case nodes =>
          scala.sys.error(s"Cannot pivot through $nodes")
      }

    inner(pivot.path.nodes, rvalue)
  }

  def interpretWrap(wrap: Wrap, rvalue: RValue): RValue = {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner(remaining: List[CPathNode], rvalue: RValue): RValue =
      (remaining, rvalue) match {
        case (Nil, rv) => RObject((wrap.name, rv))

        case (CPathField(field) :: tail, obj @ RObject(fields)) =>
          fields.get(field).fold(obj)(target =>
            RObject(fields.updated(field, inner(tail, target))))

        case (CPathIndex(idx) :: tail, arr @ RArray(elems)) =>
          if (idx < 0) {
            arr
          } else {
            val result: Option[RValue] = elems.toStream.toZipper flatMap { orig =>
              orig.move(idx).map(moved =>
                RArray(moved.update(inner(tail, moved.focus)).toStream.toList))
            }
            result.getOrElse(arr)
          }

        case (_, rv) => rv
      }

    inner(wrap.path.nodes, rvalue)
  }
}
