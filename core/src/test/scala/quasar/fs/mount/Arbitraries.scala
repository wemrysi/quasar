package quasar.fs.mount

trait Arbitraries extends
  MountConfigArbitrary with
  ConnectionUriArbitrary with
  MountsArbitrary

object Arbitraries extends Arbitraries
