/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package table

import com.precog.common._
import com.precog.common.security.APIKey

import blueeyes._, json._
import scalaz._, Scalaz._

trait BlockStoreTestModule[M[+_]] extends BaseBlockStoreTestModule[M] {
  implicit def M: Monad[M] with Comonad[M]

  type GroupId = String
  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = "groupId(" + groupId.getAndIncrement + ")"

  trait TableCompanion extends BaseBlockStoreTestTableCompanion

  object Table extends TableCompanion
}

trait BaseBlockStoreTestModule[M[+_]] extends ColumnarTableModuleTestSupport[M]
    with SliceColumnarTableModule[M]
    with StubProjectionModule[M, Slice] {

  import trans._

  implicit def M: Monad[M] with Comonad[M]

  object Projection extends ProjectionCompanion

  case class Projection(data: Stream[JValue]) extends ProjectionLike[M, Slice] {
    type Key = JArray

    private val slices = fromJson(data).slices.toStream.copoint

    val length: Long = data.length
    val xyz = slices.foldLeft(Set.empty[ColumnRef]) {
      case (acc, slice) => acc ++ slice.columns.keySet
    }
    def structure(implicit M: Monad[M]) = M.point(xyz)

    def getBlockAfter(id: Option[JArray], colSelection: Option[Set[ColumnRef]])(implicit M: Monad[M]) = M.point {
      @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
        blocks.filterNot(_.isEmpty) match {
          case x #:: xs =>
            if ((x.toJson(x.size - 1).getOrElse(JUndefined) \ "key") > id) Some(x) else findBlockAfter(id, xs)

          case _ => None
        }
      }

      val slice = id map (findBlockAfter(_, slices)) getOrElse slices.headOption

      slice map { s =>
        val s0 = new Slice {
          val size = s.size
          val columns = colSelection.map { reqCols =>
            s.columns.filter {
              case (ref @ ColumnRef(jpath, ctype), _) =>
                jpath.nodes.head == CPathField("key") || reqCols.exists { ref =>
                  (CPathField("value") \ ref.selector) == jpath && ref.ctype == ctype
                }
            }
          }.getOrElse(s.columns)
        }

        BlockProjectionData[JArray, Slice](s0.toJson(0).getOrElse(JUndefined) \ "key" --> classOf[JArray], s0.toJson(s0.size - 1).getOrElse(JUndefined) \ "key" --> classOf[JArray], s0)
      }
    }
  }

  trait BaseBlockStoreTestTableCompanion extends SliceColumnarTableCompanion

  def userMetadataView(apiKey: APIKey) = sys.error("TODO")

  def compliesWithSchema(jv: JValue, ctype: CType): Boolean = (jv, ctype) match {
    case (_: JNum, CNum | CLong | CDouble) => true
    case (JUndefined, CUndefined) => true
    case (JNull, CNull) => true
    case (_: JBool, CBoolean) => true
    case (_: JString, CString) => true
    case (JObject(fields), CEmptyObject) if fields.isEmpty => true
    case (JArray(Nil), CEmptyArray) => true
    case _ => false
  }

  def sortTransspec(sortKeys: CPath*): TransSpec1 = InnerObjectConcat(sortKeys.zipWithIndex.map {
    case (sortKey, idx) => WrapObject(
      sortKey.nodes.foldLeft[TransSpec1](DerefObjectStatic(Leaf(Source), CPathField("value"))) {
        case (innerSpec, field: CPathField) => DerefObjectStatic(innerSpec, field)
        case (innerSpec, index: CPathIndex) => DerefArrayStatic(innerSpec, index)
        case x                              => sys.error(s"Unexpected arg $x")
      },
      "%09d".format(idx)
    )
  }: _*)
}

object BlockStoreTestModule {
  def empty[M[+_]](implicit M0: Monad[M] with Comonad[M]) = new BlockStoreTestModule[M] {
    val M = M0
    val projections = Map.empty[Path, Projection]
  }
}
