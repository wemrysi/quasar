package quasar

trait Arbitraries extends
  DataArbitrary with
  TypeArbitrary

object Arbitraries extends Arbitraries
