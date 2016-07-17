package blueeyes

import org.scalacheck._, Gen._
import org.scalacheck.util.Buildable
import scala.collection.mutable.Builder
import scalaz._
import java.math.MathContext, MathContext._
import PrecogScalacheck._

object PrecogArb {
  implicit def arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary(genBigDecimal)
}

object PrecogScalacheck {
  def maxSequenceLength = 16

  implicit def liftGenerator[A](g: Gen[A]): Arbitrary[A] = Arbitrary(g)

  implicit def buildableVector[A] : Buildable[A, Vector] = new Buildable[A, Vector] {
    def builder: Builder[A, Vector[A]] = Vector.newBuilder[A]
  }

  def genFile: Gen[File] = listOfN(3, identifier map (_ take 5)) map (xs => new File(xs.mkString("/", "/", ".cooked")))

  def containerOfAtMostN[C[X] <: Traversable[X], A](maxSize: Int, g: Gen[A])(implicit b: Buildable[A, C]): Gen[C[A]] =
    sized(size => for(n <- choose(0, size min maxSize); c <- containerOfN[C, A](n, g)) yield c)

  def vectorOf[A](gen: Gen[A]): Gen[Vector[A]]                         = containerOfAtMostN[Vector, A](maxSequenceLength, gen)
  def vectorOfN[A](len: Int, gen: Gen[A]): Gen[Vector[A]]              = containerOfN(len, gen)
  def arrayOf[A: ClassManifest](g: Gen[A]): Gen[Array[A]]              = vectorOf[A](g) map (_.toArray)
  def arrayOfN[A: ClassManifest](len: Int, gen: Gen[A]): Gen[Array[A]] = vectorOfN(len, gen) map (_.toArray)

  def genInt: Gen[Int]       = Arbitrary.arbInt.arbitrary
  def genLong: Gen[Long]     = Arbitrary.arbLong.arbitrary
  def genDouble: Gen[Double] = Arbitrary.arbDouble.arbitrary
  def genBigInt: Gen[BigInt] = Arbitrary.arbBigInt.arbitrary
  def genString: Gen[String] = Arbitrary.arbString.arbitrary

  def genBigDecimal: Gen[BigDecimal] = Arbitrary.arbBigDecimal.arbitrary map (d => BigDecimal(d.bigDecimal, d.mc))

  implicit def ArbitraryOps[A](arb: Arbitrary[A]) = new {
    def ^^[B](f: A => B): Arbitrary[B]      = Arbitrary(arb.arbitrary map f)
    def >>[B](f: A => Gen[B]): Arbitrary[B] = Arbitrary(arb.arbitrary flatMap f)
  }
  implicit def ScalacheckGenOps[A](gen: Gen[A]) = new {
    def ^^[B](f: A => B): Gen[B]      = gen map f
    def >>[B](f: A => Gen[B]): Gen[B] = gen flatMap f
  }
  implicit def ScalacheckGen2Ops[A, B](gen: (Gen[A], Gen[B])) = new {
    def >>[C](f: (A, B) => C): Gen[C] = for (a <- gen._1 ; b <- gen._2) yield f(a, b)
  }
}
