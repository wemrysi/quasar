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

import slamdata.Predef.{Stream => _, _}

import cats.effect.Concurrent

import quasar.{FocusedParseInstruction, IdStatus, ParseInstruction, ParseType}
import quasar.IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.ParseInstruction.{Cartesian, Ids, Mask, Pivot, Project, Wrap}
import quasar.common.{CPath, CPathField, CPathIndex, CPathNode}
import quasar.common.data._

import scala.Predef.identity
import scala.collection.Iterator
import scala.math

import fs2.{Chunk, Pipe, Stream}

import scalaz.{NonEmptyList, Scalaz}, Scalaz._

import shims._

object RValueParseInstructionInterpreter {

  /** The chunk size used when `minUnit` isn't specified. */
  val DefaultChunkSize: Int = 1024

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def apply[F[_]: Concurrent](
      parallelism: Int,
      minUnit: Int,
      instructions: List[ParseInstruction])
      : Pipe[F, RValue, RValue] =
    instructions match {
      case Nil =>
        identity[Stream[F, RValue]]

      case Ids :: Project(CPath(CPathIndex(0))) :: t =>
        interpretIdStatus(IdOnly) andThen interpret(parallelism, minUnit, t)

      case Ids :: t =>
        interpretIdStatus(IncludeId) andThen interpret(parallelism, minUnit, t)

      case instrs =>
        interpret(parallelism, minUnit, instrs)
    }

  def interpretCartesian[F[_]: Concurrent](
      parallelism: Int,
      minUnit: Int,
      cartesian: Cartesian)
      : Pipe[F, RValue, RValue] = {

    val cartouches =
      cartesian.cartouches.mapValues(_.map(_.toNel))

    _.flatMap {
      case RObject(fields) =>
        interpretCartesian1(parallelism, minUnit, cartouches, fields)

      case _ =>
        Stream.empty
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def interpretIdStatus[F[_]](idStatus: IdStatus): Pipe[F, RValue, RValue] =
    idStatus match {
      case ExcludeId =>
        identity[Stream[F, RValue]]

      case IncludeId =>
        _.zipWithIndex map {
          case (rvalue, id) => RValue.rArray(List(RValue.rLong(id), rvalue))
        }

      case IdOnly =>
        _.scanChunks(0L) { (index, c) =>
          var idx = index
          val out = c.map { _ =>
            val id = RValue.rLong(idx)
            idx += 1
            id
          }
          (idx, out)
        }
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

  def interpretPivot(pivot: Pivot, rvalue: RValue): Iterator[RValue] = {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner(remaining: List[CPathNode], rvalue: RValue): Iterator[RValue] =
      remaining match {
        // perform the pivot
        case Nil => {
          (rvalue, pivot.structure) match {

            // pivot object
            case (RObject(fields), ParseType.Object) =>
              pivot.status match {
                case IdOnly =>
                  fields.iterator.map(kv => CString(kv._1))
                case IncludeId => // the qscript expects the results to be returned in an array
                  fields.iterator.map(kv => RArray(CString(kv._1), kv._2))
                case ExcludeId =>
                  fields.valuesIterator
              }

            // pivot array
            case (RArray(elems), ParseType.Array) =>
              pivot.status match {
                case IdOnly =>
                  elems.iterator.zipWithIndex.map(t => CLong(t._2.toLong))
                case IncludeId => // the qscript expects the results to be returned in an array
                  elems.iterator.zipWithIndex map {
                    case (elem, idx) => RArray(CLong(idx.toLong), elem)
                  }
                case ExcludeId => elems.iterator
              }

            // pivot empty object drops the row
            case (CEmptyObject, ParseType.Object) => Iterator.empty

            // pivot empty array drops the row
            case (CEmptyArray, ParseType.Array) => Iterator.empty

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

  def interpretProject(project: Project, rvalue: RValue): Option[RValue] =
    project.path.nodes.foldLeftM(rvalue) {
      case (rv, CPathField(name)) => RValue.rField1(name).getOption(rv)
      case (rv, CPathIndex(idx)) => RValue.rElement(idx).getOption(rv)
      case _ => None
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

  ////

  private object FocusedPrefix {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def unapply(instrs: List[ParseInstruction])
        : Option[(NonEmptyList[FocusedParseInstruction], List[ParseInstruction])] =
      instrs match {
        case (fpi: FocusedParseInstruction) :: t =>
          val (focused, rest) = t.span(_.isInstanceOf[FocusedParseInstruction])
          some((NonEmptyList.nels(fpi, focused.asInstanceOf[List[FocusedParseInstruction]]: _*), rest))

        case _ => none
      }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def interpret[F[_]: Concurrent](
      parallelism: Int,
      minUnit: Int,
      instructions: List[ParseInstruction])
      : Pipe[F, RValue, RValue] =
    instructions match {
      case Nil =>
        identity[Stream[F, RValue]]

      case FocusedPrefix(focused, rest) =>
        _.flatMap(rv => Stream.chunk(Chunk.array(interpretFocused(focused, rv).toArray)))
          .through(interpret(parallelism, minUnit, rest))

      case (c @ Cartesian(_)) :: rest =>
        interpretCartesian(parallelism, minUnit, c) andThen interpret(parallelism, minUnit, rest)
    }

  private def interpretFocused(fpis: NonEmptyList[FocusedParseInstruction], rvalue: RValue)
      : Iterator[RValue] =
    fpis.foldMapLeft1(interpretFocused1(_, rvalue))((rvs, i) => rvs.flatMap(interpretFocused1(i, _)))

  private def interpretFocused1(fpi: FocusedParseInstruction, rvalue: RValue)
      : Iterator[RValue] =
    fpi match {
      case Ids =>
        scala.sys.error("Ids only allowed as the first instruction.")

      case instr @ Mask(_) =>
        interpretMask(instr, rvalue).iterator

      case instr @ Pivot(_, _, _) =>
        interpretPivot(instr, rvalue)

      case instr @ Project(_) =>
        interpretProject(instr, rvalue).iterator

      case instr @ Wrap(_, _) =>
        Iterator(interpretWrap(instr, rvalue))
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

  private def interpretCartesian1[F[_]: Concurrent](
      parallelism: Int,
      minUnit: Int,
      cartouches: Map[CPathField, (CPathField, Option[NonEmptyList[FocusedParseInstruction]])],
      fields: Map[String, RValue])
      : Stream[F, RValue] = {

    /**
      * @param from1 the index within the first component to begin from
      * @param count the number of values to emit
      */
    @SuppressWarnings(Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Var",
      "org.wartremover.warts.While"))
    def cross(fieldNames: Array[String], components: Array[Array[RValue]], from1: Int, count: Int)
        : Stream[F, RValue] =
      Stream.evalUnChunk(Concurrent[F] delay {
        val size = components.length
        val rvalues = new Array[RValue](count)
        val cursors = Array.fill(size)(0)
        var fields = Map.empty[String, RValue]

        var i = size - 1

        cursors(0) = from1

        while (i >= 0) {
          fields += (fieldNames(i) -> components(i)(cursors(i)))
          i -= 1
        }

        rvalues(0) = RObject(fields)

        var total = 1
        val first1 = components.indexWhere(_.length == 1)
        val init = if (first1 > 0) first1 - 1 else components.length - 1

        while (total < count) {
          i = init

          while (i >= 0) {
            val c = components(i)
            val j = cursors(i)

            if (j == (c.length - 1)) {
              fields += (fieldNames(i) -> c(0))
              cursors(i) = 0
              i -= 1
            } else {
              fields += (fieldNames(i) -> c(j + 1))
              cursors(i) = j + 1
              i = -1
            }
          }

          rvalues(total) = RObject(fields)

          total += 1
        }

        Chunk.array(rvalues)
      })

    def ranges1(totalSize: Int, chunkSize: Int, size1: Int): Stream[F, (Int, Int)] = {
      val resultsPerTick = totalSize / size1
      val ticksPerChunk = math.max(1, chunkSize / resultsPerTick)

      Stream.ranges(0, size1, ticksPerChunk) map {
        case (b, e) => (b, (e - b) * resultsPerTick)
      }
    }

    Stream suspend {
      // TODO: these could also be in parallel
      val components: List[(String, Array[RValue])] =
        cartouches.toList flatMap {
          case (wrap, (deref, fpis)) =>
            fields.get(deref.name)
              .map(rv => (wrap.name, fpis.fold(Array(rv))(interpretFocused(_, rv).toArray)))
              .toList
        }

      val sorted: List[(String, Array[RValue])] =
        components.filter(_._2.nonEmpty) sortWith {
          case ((_, leftRvs), (_, rightRvs)) => leftRvs.length > rightRvs.length
        }

      val penabled = parallelism > 0 && minUnit > 0

      val resultSize: Int =
        sorted.foldLeft(1) {
          case (acc, (_, rvs)) => acc * rvs.length
        }

      val (names, values) =
        sorted.unfzip.bimap(_.toArray, _.toArray)

      if (sorted.forall(_._2.isEmpty)) {
        Stream.empty
      } else if (penabled && resultSize > minUnit * 2 && values.length > 1) { // we go parallel
        val numberOfJobs: Int =
          math.min(parallelism, resultSize / minUnit)

        val jobSize: Int =
          math.ceil(resultSize.toDouble / numberOfJobs.toDouble).toInt

        val chunksPerJob: Int =
          math.ceil(jobSize.toDouble / minUnit.toDouble).toInt

        val chunks =
          ranges1(resultSize, minUnit, values(0).length)
            .map { case (b, c) => cross(names, values, b, c) }

        val coalesced =
          if (chunksPerJob > 1)
            chunks
              .chunkN(chunksPerJob)
              .map(_.foldLeft[Stream[F, RValue]](Stream.empty)((s, ss) => ss ++ s))
          else
            chunks

        coalesced.parJoin(numberOfJobs)
      } else {
        ranges1(resultSize, DefaultChunkSize, values(0).length) flatMap {
          case (b, c) => cross(names, values, b, c)
        }
      }
    }
  }
}
