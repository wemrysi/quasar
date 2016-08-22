package ygg.table

object yggConfig {
  def hashJoins         = true
  def sortBufferSize    = 1000
  def maxSliceSize: Int = 10

  // This is a slice size that we'd like our slices to be at least as large as.
  def minIdealSliceSize: Int = maxSliceSize / 4

  // This is what we consider a "small" slice. This may affect points where
  // we take proactive measures to prevent problems caused by small slices.
  def smallSliceSize: Int = 3

  def maxSaneCrossSize: Long = 2400000000L // 2.4 billion
}
