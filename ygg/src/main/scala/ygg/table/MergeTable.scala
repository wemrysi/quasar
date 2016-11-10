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

package ygg.table

import ygg._, common._
import trans._
import scalaz._, Scalaz._

object MergeTable {
  type M[+X]     = Need[X]
  type Key       = Seq[RValue]
  type KeySchema = Seq[CPathField]
  final case class IndexedSource(groupId: GroupId, index: TableIndex, keySchema: KeySchema)

  /**
    * Merge controls the iteration over the table of group key values.
    */
  def apply[T](grouping: GroupingSpec[T])(body: (RValue, GroupId => M[T]) => M[T]): M[T] = {
    import grouping._
    import GroupKeySpec.{ dnf, toVector }

    def sources(spec: GroupKeySpec): Seq[GroupKeySpecSource] = (spec: @unchecked) match {
      case GroupKeySpecAnd(left, right) => sources(left) ++ sources(right)
      case src: GroupKeySpecSource      => Vector(src)
    }
    def sourcesOf(gs: GroupingSpec[T]): Vector[GroupingSource[T]] = gs match {
      case x: GroupingSource[T]                    => Vector(x)
      case GroupingAlignment(_, _, left, right, _) => sourcesOf(left) ++ sourcesOf(right)
    }

    def mkProjections(spec: GroupKeySpec) =
      toVector(dnf(spec)) map (sources(_) map (s => s.key -> s.spec))

    (for {
      source              <- sourcesOf(grouping)
      groupKeyProjections <- mkProjections(source.groupKeySpec)
      disjunctGroupKeyTransSpecs = groupKeyProjections.map { case (key, spec) => spec }
    }
    yield {
      import source._
      TableIndex.createFromTable[T](table, disjunctGroupKeyTransSpecs, targetTrans.getOrElse(ID)).map { index =>
        IndexedSource(groupId, index, groupKeyProjections.map(_._1))
      }
    }).sequence.flatMap { sourceKeys =>
      val fullSchema = sourceKeys.flatMap(_.keySchema).distinct

      val indicesGroupedBySource = sourceKeys.groupBy(_.groupId).mapValues(_.map(y => (y.index, y.keySchema)).toVector).values.toVector

      def unionOfIntersections(indicesGroupedBySource: Seq[Seq[TableIndex -> KeySchema]]): Set[Key] = {
        def allSourceDNF[T](l: Seq[Seq[T]]): Seq[Seq[T]] = l match {
          case Seq()       => Seq()
          case hd +: Seq() => hd.map(Seq(_))
          case hd +: tl    => hd flatMap (disjunctHd => allSourceDNF(tl) map (disjunctTl => disjunctHd +: disjunctTl))
        }
        def normalizedKeys(index: TableIndex, keySchema: KeySchema): Set[Key] = {
          val schemaMap = for (k <- fullSchema) yield keySchema.indexOf(k)
          for (key       <- index.getUniqueKeys)
            yield for (k <- schemaMap) yield if (k == -1) CUndefined else key(k)
        }

        def intersect(keys0: Set[Key], keys1: Set[Key]): Set[Key] = {
          def consistent(key0: Key, key1: Key): Boolean =
            (key0 zip key1).forall {
              case (k0, k1) => k0 == k1 || k0 == CUndefined || k1 == CUndefined
            }

          def merge(key0: Key, key1: Key): Key =
            (key0 zip key1).map {
              case (k0, CUndefined) => k0
              case (_, k1)          => k1
            }

          // TODO: This "mini-cross" is much better than the
          // previous mega-cross. However in many situations we
          // could do even less work. Consider further optimization
          // (e.g. when one key schema is a subset of the other).

          // Relatedly it might make sense to topologically sort the
          // Indices by their keyschemas so that we end up intersecting
          // key with their subset.
          keys0.flatMap { key0 =>
            keys1.flatMap(key1 => if (consistent(key0, key1)) Some(merge(key0, key1)) else None)
          }
        }

        allSourceDNF(indicesGroupedBySource).foldLeft(Set.empty[Key]) {
          case (acc, intersection) =>
            val hd = normalizedKeys(intersection.head._1, intersection.head._2)
            acc | intersection.tail.foldLeft(hd) {
              case (keys0, (index1, schema1)) =>
                val keys1 = normalizedKeys(index1, schema1)
                intersect(keys0, keys1)
            }
        }
      }

      def jValueFromGroupKey(key: Seq[RValue], cpaths: Seq[CPathField]): RValue = {
        val items = (cpaths zip key).map(t => (t._1.name, t._2))
        RObject(items.toMap)
      }

      val groupKeys: Set[Key] = unionOfIntersections(indicesGroupedBySource)

      // given a groupKey, return an M[Table] which represents running
      // the evaluator on that subgroup.
      def evaluateGroupKey(groupKey: Key): M[T] = {
        import grouping._
        val groupKeyTable = jValueFromGroupKey(groupKey, fullSchema)

        def map(gid: GroupId): M[T] = {
          val subTableProjections = (sourceKeys
            .filter(_.groupId == gid)
            .map { indexedSource =>
              val keySchema           = indexedSource.keySchema
              val projectedKeyIndices = for (k <- fullSchema) yield keySchema.indexOf(k)
              (indexedSource.index, projectedKeyIndices, groupKey)
            })
            .toList

          Need(TableIndex.joinSubTables[T](subTableProjections).normalize) // TODO: normalize necessary?
        }

        body(groupKeyTable, map)
      }

      // TODO: this can probably be done as one step, but for now
      // it's probably fine.
      val tables: StreamT[Need, T] = StreamT.unfoldM(groupKeys.toList) {
        case k :: ks => evaluateGroupKey(k).map(t => some(t -> ks))
        case Nil     => Need(None)
      }

      Need(lazyTable(tables flatMap (_.slices), UnknownSize))
    }
  }
}
