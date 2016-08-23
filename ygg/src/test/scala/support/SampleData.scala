package ygg.tests

import ygg.common._
import TestSupport._
import ygg.json._
import ygg.table._
import CValueGenerators.JSchema

import scala.collection.generic.CanBuildFrom
import scala.util.Random

final case class SampleData(data: Stream[JValue], schema: Option[Int -> JSchema] = None) {
  override def toString = {
    "SampleData: \ndata = " + data.map(_.toString.replaceAll("\n", "\n  ")).mkString("[\n  ", ",\n  ", "]\n") +
      "\nschema: " + schema
  }

  def sorted(implicit z: Ord[JValue]): SampleData                = transform(_.sorted)
  def sortBy[B: Ord](f: JValue => B): SampleData                 = transform(_ sortBy f)
  def transform(f: Stream[JValue] => Stream[JValue]): SampleData = copy(data = f(data))
}

object SampleData extends CValueGenerators {
  implicit def keyOrder[A]: Ord[Identities -> A] = tupledIdentitiesOrder[A](IdentitiesOrder)

  def sample(schema: Int => Gen[JSchema]): Arbitrary[SampleData] = Arbitrary(
    for {
      depth           <- choose(0, 1)
      jschema         <- schema(depth)
      (idCount, data) <- genEventColumns(jschema)
    } yield {
      SampleData(
        data.sorted.toStream flatMap {
          // Sometimes the assembly process will generate overlapping values which will
          // cause RuntimeExceptions in JValue.unsafeInsert. It's easier to filter these
          // out here than prevent it from happening in the first place.
          case (ids, jv) => try { Some(toRecord(ids, assemble(jv))) } catch { case _: RuntimeException => None }
        },
        Some((idCount, jschema))
      )
    }
  )

  def distinctBy[T, C[X] <: Seq[X], S](c: C[T])(key: T => S)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()
    val seen    = scmSet[S]()

    for (t <- c) {
      if (!seen(key(t))) {
        builder += t
        seen += key(t)
      }
    }

    builder.result
  }

  def randomSubset[T, C[X] <: Seq[X], S](c: C[T], freq: Double)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()

    for (t <- c)
      if (Random.nextDouble < freq)
        builder += t

    builder.result
  }

  def sort(sd: Arbitrary[SampleData]): Arbitrary[SampleData]         = sd ^^ (_.sorted)
  def shuffle(sd: Arbitrary[SampleData]): Arbitrary[SampleData]      = sd ^^ (_ transform Random.shuffle)
  def distinct(sd: Arbitrary[SampleData]): Arbitrary[SampleData]     = sd ^^ (_ transform (_.distinct))
  def distinctKeys(sd: Arbitrary[SampleData]): Arbitrary[SampleData] = sd ^^ (_ transform (d => distinctBy(d)(_ \ "keys")))

  def distinctValues(sample: Arbitrary[SampleData]): Arbitrary[SampleData] =
    sample ^^ (sd => SampleData(distinctBy(sd.data)(_ \ "value"), sd.schema))

  def duplicateRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = sample ^^ { sd =>
    val rows       = sd.data
    val duplicates = randomSubset(rows, 0.25)
    SampleData(Random.shuffle(rows ++ duplicates), sd.schema)
  }

  def undefineRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = sample ^^ { sd =>
    val rows = sd.data map (row => if (Random.nextDouble < 0.25) JUndefined else row)
    SampleData(rows, sd.schema)
  }

  def undefineRowsForColumn(sample: Arbitrary[SampleData], path: JPath): Arbitrary[SampleData] = sample ^^ { sd =>
    val rows = sd.data map (row =>
      row get path match {
        case null | JUndefined => row
        case _                 => row.set(path, JUndefined)
      }
    )
    SampleData(rows, sd.schema)
  }
}
