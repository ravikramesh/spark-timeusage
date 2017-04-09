package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  def initializeTimeUsage(): Boolean =
    try {
      TimeUsage
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeTimeUsage(), " -- did you fill in all the values in initializeTimeUsage ?")
    import TimeUsage._
    spark.stop()
  }

  test(" test read functionality ") {
    assert(initializeTimeUsage(), " -- test read method? ")
    import TimeUsage._
    val (columns, initDf) = read("/timeusage/atussum.csv")
    assert(columns.size == 455)
    assert(columns(0)== "tucaseid", "First column is not tucaseid")
  }


}
