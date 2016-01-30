package quasar.fs.mount

trait Arbitraries extends
  MountConfigArbitrary with
  ConnectionUriArbitrary with
  MountsArbitrary with
  MountingsConfigArbitrary

object Arbitraries extends Arbitraries
