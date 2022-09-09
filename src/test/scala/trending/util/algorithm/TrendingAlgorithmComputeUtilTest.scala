package trending.util.algorithm

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import trending.algorithm.TrendingAlgorithmComputeUtil.computeMannKendallScore

class TrendingAlgorithmComputeUtilTest extends AnyFlatSpec with Matchers {

  it should "check Mann-Kendall sum for an array sorted in increasing order" in {
    val inputArray: Array[Long] = Array(1, 2, 3, 4, 5)

    computeMannKendallScore(inputArray) should equal(10)
  }

  it should "check Mann-Kendall sum for an unsorted array" in {
    val inputArray: Array[Long] = Array(1, 2, 3, 4, 1)

    computeMannKendallScore(inputArray) should equal(3)
  }

  it should "check Mann-Kendall sum for empty array" in {
    computeMannKendallScore(Array.emptyLongArray) should equal(0)
  }

}
