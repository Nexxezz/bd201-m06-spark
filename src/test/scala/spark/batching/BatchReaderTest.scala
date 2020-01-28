package spark.batching

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

object BatchReaderTest extends FunSuite with SharedSparkContext{
  test("test initializing spark context") {
    val list = List(4, 8, 15, 16)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }
}
