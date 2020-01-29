package spark.batching

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.FunSuite

object BatchReaderTest extends FunSuite with SharedSparkContext{
  test("test initializing spark context") {
    val list = List(4, 8, 15, 16)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }
}
class test extends FunSuite with DataFrameSuiteBase {
  test("simple test") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input1 = sc.parallelize(List[(Int, Double)]((1, 1.1), (2, 2.2), (3, 3.3))).toDF
    val input2 = sc.parallelize(List[(Int, Double)]((1, 1.2), (2, 2.3), (3, 3.4))).toDF
    assertDataFrameApproximateEquals(input1, input2, 0.11) // equal

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(input1, input2, 0.05) // not equal
    }
  }
}
