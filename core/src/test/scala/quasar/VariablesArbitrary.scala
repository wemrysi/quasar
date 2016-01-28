package quasar

import quasar.Predef._
import quasar.sql.ExprArbitrary

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.{arbitrary => arb}

trait VariablesArbitrary {
  implicit val arbitraryVarName: Arbitrary[VarName] =
    Arbitrary(arb[String].map(VarName(_)))

  implicit val arbitraryVarValue: Arbitrary[VarValue] =
    Arbitrary(ExprArbitrary.constExprGen.map(expr => VarValue(expr.toString)))

  implicit val arbitraryVariables: Arbitrary[Variables] =
    Arbitrary(arb[Map[VarName, VarValue]].map(Variables(_)))
}

object VariablesArbitrary extends VariablesArbitrary
