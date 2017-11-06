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

package quasar.physical.rdbms.model

import quasar.{Data, DataCodec, Qspec}
import slamdata.Predef._
import TableModel._
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.specs2.scalacheck.Parameters

import scalaz._
import Scalaz._

class TableModelTest extends Qspec {

  implicit val codec: DataCodec = DataCodec.Precise
  implicit val params: Parameters = Parameters(minTestsOk = 1000)


  def data(str: String): Data =
    DataCodec.parse(str).valueOr(err => scala.sys.error(err.shows))
  
  def permutations[T](v: Vector[T]): Gen[Vector[T]] = {
    val perms = v.permutations.toList
    val permStream = Stream.continually(perms).flatMap(_.toStream).toIterator
    Gen.delay(permStream.next())
  }

  "Table Model" should {

    "fail for empty vector" in {
      TableModel.fromData(Vector.empty) should beLeftDisjunction
    }

    "devise columnar schema for a single row" in {
      val row = data("""{"name":"John","surname":"Smith","birthYear":1980}""")
      TableModel.fromData(Vector(row)) should beRightDisjunction(
        ColumnarTable(
          ISet.fromList(List(ColumnDesc("name", StringCol),
            ColumnDesc("birthYear", IntCol),
            ColumnDesc("surname", StringCol)))))
    }

    "devise columnar schema for homogenous data" in {
      val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
      TableModel.fromData(Vector(row, row, row, row, row, row)) should beRightDisjunction(
        ColumnarTable(
          ISet.fromList(List(ColumnDesc("age", IntCol),
            ColumnDesc("keys", JsonCol),
            ColumnDesc("id", StringCol)))))
    }

    "devise columnar schema when extra columns appear" in {
        val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
        val updatedRow1 =
          data("""{"age": 35,"id":"a748abf","keys": [], "extra-col" : "v1"}""")
        val updatedRow2 = data(
          """{"age": 45, "another-extra-col": "v2", "id":"11","keys": [222]}""")

        forAll(permutations(Vector(row,
          row,
          updatedRow1,
          row,
          updatedRow2,
          row,
          row))) { rows =>

        TableModel.fromData(rows) should beRightDisjunction(
          ColumnarTable(ISet.fromList(List(
            ColumnDesc("age", IntCol),
            ColumnDesc("keys", JsonCol),
            ColumnDesc("id", StringCol),
            ColumnDesc("extra-col", StringCol),
            ColumnDesc("another-extra-col", StringCol)
          ))))
      }
    }

    "devise columnar schema when some columns get nulled" in {
        val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
        val updatedRow1 = data("""{"id":"a748abf","keys": []}""")

        forAll(permutations(
          Vector(row, row, updatedRow1, row, row))) { rows =>

        TableModel.fromData(rows) should beRightDisjunction(
          ColumnarTable(
            ISet.fromList(List(ColumnDesc("age", IntCol),
              ColumnDesc("keys", JsonCol),
              ColumnDesc("id", StringCol)))))
      }
    }

    "devise columnar schema when some columns get nulled and some added" in {
        val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
        val updatedRow1 = data("""{"id":"a748abf","keys": []}""")
        val updatedRow2 = data("""{"extra": {"nested1": "val"}, "id":"a748abf","keys": []}""")

        forAll(permutations(Vector(row,
          row,
          updatedRow1,
          row,
          row,
          updatedRow2))) { rows =>

        TableModel.fromData(rows) should beRightDisjunction(
          ColumnarTable(
            ISet.fromList(
              List(ColumnDesc("age", IntCol),
                ColumnDesc("keys", JsonCol),
                ColumnDesc("id", StringCol),
                ColumnDesc("extra", JsonCol)))))
      }
    }

    "devise columnar schema when all columns are nulled and some row has only new columns" in {
        val row = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
        val updatedRow1 = data("""{}""")
        val updatedRow2 = data("""{"extra": 35, "extra2": "str value"}""")

      forAll(permutations(Vector(row,
        row,
        updatedRow1,
        row,
        row,
        updatedRow2))) { rows =>

        TableModel.fromData(rows) should beRightDisjunction(
          ColumnarTable(
            ISet.fromList(
              List(ColumnDesc("age", IntCol),
                ColumnDesc("keys", JsonCol),
                ColumnDesc("id", StringCol),
                ColumnDesc("extra", IntCol),
                ColumnDesc("extra2", StringCol)))))
      }
    }

    "devise json schema on encountering incompatible data types" in {
      val row1 = data("""{"age": 25,"id":"748abf","keys": [4, 5, 6, 7]}""")
      val row2 = data("""{"age": "25","id":"648abf","keys": [4, 5, 6, 7]}""")

      forAll(permutations(Vector(row1, row2, row2, row2, row1))) { rows =>

        TableModel.fromData(rows) should beRightDisjunction(JsonTable)
      }
    }

    "treat nulls as 'always compatible' column types" in {
      val row1 = data("""{"age": 25,"id":"748abf", "name": "Bob"}""")
      val row2 = data("""{"age": 35,"id":"648abf", "name": null}""")

      forAll(permutations(Vector(row1, row2, row2, row2, row1))) { rows =>

        TableModel.fromData(rows) should beRightDisjunction(
          ColumnarTable(
            ISet.fromList(
              List(ColumnDesc("age", IntCol),
                ColumnDesc("id", StringCol),
                ColumnDesc("name", StringCol)))))
      }
    }
  }
}
