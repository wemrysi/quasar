package ygg.tests

import ygg.internal._
import scala.collection.mutable.Builder
import ygg.data._

object TestSupport extends TestSupport
trait TestSupport extends ScalacheckSupport with SpecsSupport {
  implicit def arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary(genBigDecimal)
}

trait SpecsSupport {
  import org.specs2._, matcher._

  type ScalaCheck = org.specs2.ScalaCheck
  val SpecsFailure = org.specs2.execute.Failure
  type MatchResult[+A]    = org.specs2.matcher.MatchResult[A]
  type SpecsFailure       = org.specs2.execute.Failure
  type SpecsResult        = org.specs2.execute.Result
  type Specification      = org.specs2.mutable.Specification
  type SpecificationLike  = org.specs2.mutable.SpecificationLike
  type ThrownExpectations = org.specs2.matcher.ThrownExpectations

  trait SpecificationHelp extends SpecificationLike with ThrownMessages {
    def haveAllElementsLike[A](pf: PartialFunction[A, Boolean]): ContainWithResult[A] = contain(like[A]({ case x if pf.isDefinedAt(x) && pf(x) => ok })).forall
  }
}

trait ScalacheckSupport {
  val Arbitrary = org.scalacheck.Arbitrary
  val Gen       = org.scalacheck.Gen
  val Pretty    = org.scalacheck.util.Pretty
  val Prop      = org.scalacheck.Prop

  type Arbitrary[A]            = org.scalacheck.Arbitrary[A]
  type Buildable[Elem, Result] = org.scalacheck.util.Buildable[Elem, Result]
  type Gen[+A]                 = org.scalacheck.Gen[A]
  type Pretty                  = org.scalacheck.util.Pretty
  type Prop                    = org.scalacheck.Prop

  import Gen.{ listOfN, containerOfN, identifier, sized, oneOf, frequency }

  def choose(lo: Int, hi: Int): Gen[Int]    = Gen.choose(lo, hi)
  def choose(lo: Long, hi: Long): Gen[Long] = Gen.choose(lo, hi)
  def const[A](value: A): Gen[A]            = Gen.const(value)

  def arbitrary[A](implicit z: Arbitrary[A]): Gen[A] = z.arbitrary

  def maxSequenceLength = 16

  implicit def liftGenerator[A](g: Gen[A]): Arbitrary[A] = Arbitrary(g)

  implicit def buildableVector[A]: Buildable[A, Vector[A]] = new Buildable[A, Vector[A]] {
    def builder: Builder[A, Vector[A]] = Vector.newBuilder[A]
  }

  def genFile: Gen[File] = listOfN(3, identifier map (_ take 5)) map (xs => new File(xs.mkString("/", "/", ".cooked")))

  def genBitSet(size: Int): Gen[BitSet] = {
    genBool * size ^^ { bools =>
      doto(new BitSet) { bs =>
        bools.indices foreach { i =>
          if (bools(i))
            bs set i
        }
      }
    }
  }

  def containerOfAtMostN[C[X] <: Traversable[X], A](maxSize: Int, g: Gen[A])(implicit b: Buildable[A, C[A]]): Gen[C[A]] =
    sized(size => for (n <- choose(0, size min maxSize); c <- containerOfN[C, A](n, g)) yield c)

  def arrayOf[A: CTag](gen: Gen[A]): Gen[Array[A]] = vectorOf(gen) ^^ (_.toArray)
  def vectorOf[A](gen: Gen[A]): Gen[Vector[A]]     = containerOfAtMostN[Vector, A](maxSequenceLength, gen)
  def listOf[A](gen: Gen[A]): Gen[List[A]]         = containerOfAtMostN[List, A](maxSequenceLength, gen)
  def setOf[A](gen: Gen[A]): Gen[Set[A]]           = containerOfAtMostN[Set, A](maxSequenceLength, gen)

  def arrayOfN[A: CTag](len: Int, gen: Gen[A]): Gen[Array[A]] = vectorOfN(len, gen) ^^ (_.toArray)
  def vectorOfN[A](len: Int, gen: Gen[A]): Gen[Vector[A]]     = containerOfN[Vector, A](len, gen)
  def setOfN[A](len: Int, gen: Gen[A]): Gen[Set[A]]           = containerOfN[Set, A](len, gen)

  // !!! Should take an Eq[K].
  def mapOfN[K, V](len: Int, k: Gen[K], v: Gen[V]): Gen[sciMap[K, V]] =
    (setOfN(len, k) -> listOfN(len, v)) >> (_ zip _ toMap)

  def genIndex(size: Int): Gen[Int] = choose(0, size - 1)
  def genBool: Gen[Boolean]         = oneOf(true, false)
  def genInt: Gen[Int]              = choose(Int.MinValue, Int.MaxValue)
  def genLong: Gen[Long]            = choose(Long.MinValue, Long.MaxValue)
  def genDouble: Gen[Double]        = Arbitrary.arbDouble.arbitrary
  def genBigInt: Gen[BigInt]        = Arbitrary.arbBigInt.arbitrary
  def genString: Gen[String]        = Arbitrary.arbString.arbitrary
  def genIdentifier: Gen[String]    = identifier filter (_ != null)
  def genPosLong: Gen[Long]         = choose(1L, Long.MaxValue)
  def genPosInt: Gen[Int]           = choose(1, Int.MaxValue)

  // BigDecimal *isn't* arbitrary precision!  AWESOME!!!
  // (and scalacheck's BigDecimal gen will overflow at random)
  def genBigDecimal: Gen[BigDecimal] = genBigDecimal(genExponent = genBigDecExponent)
  def genBigDecExponent              = choose(-50000, 50000)
  def genBigDecimal(genExponent: Gen[Int]): Gen[BigDecimal] = (genLong, genExponent) >> { (mantissa, exponent) =>
    def adjusted = (
      if (exponent.toLong + mantissa.toString.length >= Int.MaxValue.toLong)
        exponent - mantissa.toString.length
      else if (exponent.toLong - mantissa.toString.length <= Int.MinValue.toLong)
        exponent + mantissa.toString.length
      else
        exponent
    )
    decimal(unscaledVal = mantissa, scale = adjusted)
  }

  def genBitSet: Gen[BitSet] = listOf(0 upTo 500) ^^ (Bits(_))

  def genSparseBitSet: Gen[Codec[BitSet] -> BitSet] = {
    (0 upTo 500) >> { size =>
      val codec = Codec.SparseBitSetCodec(size)
      if (size > 0)
        listOf(0 upTo size - 1) ^^ (bits => codec -> Bits(bits))
      else
        const(codec -> Bits())
    }
  }
  def genSparseRawBitSet: Gen[Codec[RawBitSet] -> RawBitSet] = {
    (0 upTo 500) >> { size =>
      val codec: Gen[Codec[RawBitSet]] = Codec.SparseRawBitSetCodec(size)
      val bits: Gen[RawBitSet] = size match {
        case 0 => const(RawBitSet create 0)
        case n => listOf(0 upTo size - 1) ^^ (bits => doto(RawBitSet create size)(bs => bits foreach (b => RawBitSet.set(bs, b))))
      }
      (codec, bits).zip
    }
  }

  implicit class ScalacheckIntOps(private val n: Int) {
    def upTo(end: Int): Gen[Int] = choose(n, end)
  }

  implicit class ArbitraryOps[A](arb: Arbitrary[A]) {
    def ^^[B](f: A => B): Arbitrary[B]      = Arbitrary(arb.arbitrary map f)
    def >>[B](f: A => Gen[B]): Arbitrary[B] = Arbitrary(arb.arbitrary flatMap f)
  }
  implicit class ScalacheckGenOps[A](gen: Gen[A]) {
    def ^^[B](f: A => B): Gen[B]      = gen map f
    def >>[B](f: A => Gen[B]): Gen[B] = gen flatMap f

    def *(gn: Gen[Int]): Gen[List[A]] = gn >> (this * _)
    def *(n: Int): Gen[List[A]]       = listOfN(n, gen)
    def list: Gen[List[A]]            = listOf(gen)
    def optional: Gen[Option[A]] = frequency(
      1  -> None,
      10 -> (gen map (x => Some(x)))
    )
  }
  implicit class ScalacheckGen2Ops[A, B](gen: (Gen[A], Gen[B])) {
    def >>[C](f: (A, B) => C): Gen[C] = for (a <- gen._1; b <- gen._2) yield f(a, b)
    def zip: Gen[A -> B]              = >>(scala.Tuple2(_, _))
  }
  implicit class ScalacheckGen3Ops[A, B, C](gen: (Gen[A], Gen[B], Gen[C])) {
    def >>[D](f: (A, B, C) => D): Gen[D] = for (a <- gen._1; b <- gen._2; c <- gen._3) yield f(a, b, c)
    def zip: Gen[(A, B, C)]              = >>(scala.Tuple3(_, _, _))
  }
}
