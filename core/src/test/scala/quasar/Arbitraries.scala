package quasar

trait Arbitraries extends
  DataArbitrary with
  TypeArbitrary with
  VariablesArbitrary

object Arbitraries extends Arbitraries
