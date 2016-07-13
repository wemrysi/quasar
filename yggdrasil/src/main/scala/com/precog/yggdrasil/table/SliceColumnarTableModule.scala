// /*
//  *  ____    ____    _____    ____    ___     ____
//  * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
//  * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
//  * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
//  * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
//  *
//  * This program is free software: you can redistribute it and/or modify it under the terms of the
//  * GNU Affero General Public License as published by the Free Software Foundation, either version
//  * 3 of the License, or (at your option) any later version.
//  *
//  * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
//  * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
//  * the GNU Affero General Public License for more details.
//  *
//  * You should have received a copy of the GNU Affero General Public License along with this
//  * program. If not, see <http://www.gnu.org/licenses/>.
//  *
//  */
// package com.precog.yggdrasil
// package table

// import blueeyes._
// import com.precog.common._, security._
// import com.precog.bytecode._

// import scalaz._
// import scalaz.std.list._
// import scalaz.syntax.monad._
// import scalaz.syntax.traverse._

// //FIXME: This is only used in test at this point, kill with fire in favor of VFSColumnarTableModule
// trait SliceColumnarTableModule[M[+ _]] extends BlockStoreColumnarTableModule[M] with ProjectionModule[M, Slice] {
//   type TableCompanion <: SliceColumnarTableCompanion

//   trait SliceColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
//     def load(table: Table, apiKey: APIKey, tpe: JType): EitherT[M, vfs.ResourceError, Table] = EitherT.right {
//       for {
//         paths <- pathsM(table)
//         projections <- paths.toList.traverse(Projection(_)).map(_.flatten)
//         totalLength = projections.map(_.length).sum
//       } yield {
//         def slices(proj: Projection, constraints: Option[Set[ColumnRef]]): StreamT[M, Slice] = {
//           StreamT.unfoldM[M, Slice, Option[proj.Key]](None) { key =>
//             proj.getBlockAfter(key, constraints).map { b =>
//               b.map {
//                 case BlockProjectionData(_, maxKey, slice) =>
//                   (slice, Some(maxKey))
//               }
//             }
//           }
//         }

//         val stream = projections.foldLeft(StreamT.empty[M, Slice]) { (acc, proj) =>
//           // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
//           val constraints: M[Option[Set[ColumnRef]]] = proj.structure.map { struct =>
//             Some(Schema.flatten(tpe, struct.toList).toSet)
//           }

//           acc ++ StreamT.wrapEffect(constraints map { c =>
//             slices(proj, c)
//           })
//         }

//         Table(stream, ExactSize(totalLength))
//       }
//     }
//   }
// }
