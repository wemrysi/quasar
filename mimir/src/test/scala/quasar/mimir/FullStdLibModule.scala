package quasar.mimir

trait FullStdLibModule[M[+_]] extends StdLibOpFinderModule[M]
    with ReductionFinderModule[M]
    with EvaluatorModule[M] {
  trait Lib extends StdLibOpFinder with StdLib
  object library extends Lib
}
