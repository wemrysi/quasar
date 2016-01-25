package quasar

trait CoreArbitraries extends
  Arbitraries with
  config.Arbitraries with
  fs.Arbitraries with
  fs.mount.Arbitraries with
  sql.Arbitraries

object CoreArbitraries extends CoreArbitraries
