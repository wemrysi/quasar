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
package quasar.ygg
package table

import com.precog.common._
import com.precog.common.security.APIKey
import blueeyes._, json._
import scalaz._, Scalaz._
import com.precog.bytecode.JType

trait BlockStoreTestModule extends ColumnarTableModuleTestSupport with BlockStoreColumnarTableModule {
  def projections: Map[Path, Projection]

  trait SliceColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, apiKey: APIKey, tpe: JType): EitherT[Need, ResourceError, Table] = EitherT.right {
      for {
        paths       <- pathsM(table)
        projections <- paths.toList.traverse(Projection(_)).map(_.flatten)
        totalLength = projections.map(_.length).sum
      } yield {
        def slices(proj: Projection, constraints: Option[Set[ColumnRef]]): StreamT[Need, Slice] = {
          StreamT.unfoldM[Need, Slice, Option[proj.Key]](None) { key =>
            proj.getBlockAfter(key, constraints).map { b =>
              b.map {
                case BlockProjectionData(_, maxKey, slice) =>
                  (slice, Some(maxKey))
              }
            }
          }
        }

        val stream = projections.foldLeft(StreamT.empty[Need, Slice]) { (acc, proj) =>
          // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
          val constraints: M[Option[Set[ColumnRef]]] = proj.structure.map { struct =>
            Some(Schema.flatten(tpe, struct.toList).toSet)
          }

          acc ++ StreamT.wrapEffect(constraints map { c =>
            slices(proj, c)
          })
        }

        Table(stream, ExactSize(totalLength))
      }
    }
  }
  import trans._

  type GroupId = String
  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId      = "groupId(" + groupId.getAndIncrement + ")"

  trait TableCompanion extends BaseBlockStoreTestTableCompanion

  object Table extends TableCompanion

  object Projection {
    def apply(path: Path): Need[Option[Projection]] = Need(projections get path)
  }
  case class Projection(val data: Stream[JValue]) extends ProjectionLike {
    type Key = JArray

    private val slices      = fromJson(data).slices.toStream.copoint
    val length: Long        = data.length.toLong
    val xyz: Set[ColumnRef] = slices.foldLeft(Set[ColumnRef]())(_ ++ _.columns.keySet)

    def structure = Need(xyz)

    def getBlockAfter(id: Option[JArray], colSelection: Option[Set[ColumnRef]]) = Need {
      @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
        blocks.filterNot(_.isEmpty) match {
          case x #:: xs => if ((x.toJson(x.size - 1).getOrElse(JUndefined) \ "key") > id) Some(x) else findBlockAfter(id, xs)
          case _        => None
        }
      }

      val slice = id map (findBlockAfter(_, slices)) getOrElse slices.headOption

      slice map { s =>
        val s0 = Slice(s.size, {
          colSelection.map { reqCols =>
            s.columns.filter {
              case (ref @ ColumnRef(jpath, ctype), _) =>
                jpath.nodes.head == CPathField("key") || reqCols.exists { ref =>
                  (CPathField("value") \ ref.selector) == jpath && ref.ctype == ctype
                }
            }
          }.getOrElse(s.columns)
        })

        BlockProjectionData[JArray](
          s0.toJson(0).getOrElse(JUndefined) \ "key" --> classOf[JArray],
          s0.toJson(s0.size - 1).getOrElse(JUndefined) \ "key" --> classOf[JArray],
          s0)
      }
    }
  }

  trait BaseBlockStoreTestTableCompanion extends SliceColumnarTableCompanion

  def userMetadataView(apiKey: APIKey) = sys.error("TODO")

  def compliesWithSchema(jv: JValue, ctype: CType): Boolean = (jv, ctype) match {
    case (_: JNum, CNum | CLong | CDouble) => true
    case (JUndefined, CUndefined)          => true
    case (JNull, CNull)                    => true
    case (_: JBool, CBoolean)              => true
    case (_: JString, CString)             => true
    case (JObject(fields), CEmptyObject)   => fields.isEmpty
    case (JArray(Nil), CEmptyArray)        => true
    case _                                 => false
  }

  def sortTransspec(sortKeys: CPath*): TransSpec1 =
    InnerObjectConcat(sortKeys.zipWithIndex.map {
      case (sortKey, idx) =>
        WrapObject(
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
  object empty extends BlockStoreTestModule {
    val projections = Map[Path, Projection]()
  }
}
