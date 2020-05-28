package audience

import audience.Members._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

class MembersSpec extends FunSuite with Matchers with MockitoSugar {

  private val conf = new SparkConf().setMaster("local").setAppName("Audience")
  implicit val sc: SparkContext = SparkSession.builder().config(conf).getOrCreate().sparkContext
  implicit val basePath = "src/test/resources"


  test("getMemberDetails (one day) can read adds") {

    val date = "2017/12/01"
    val res = getMemberDetails(date,17)

    val expectedResult = List(MemberDetails("001",date,Addition),
                              MemberDetails("002",date,Addition),
                              MemberDetails("003",date,Addition))

    res.collect().toList should equal(expectedResult)
  }

  test("getMemberDetails (one day) can read removals") {

    val date = "2017/12/01"
    val res = getMemberDetails(date,-17)

    val expectedResult = List(MemberDetails("004",date,Removal),
                              MemberDetails("005",date,Removal),
                              MemberDetails("006",date,Removal),
                              MemberDetails("007",date,Removal))

    res.collect().toList.sortBy(_.memberNo) should equal(expectedResult)
  }

  test("getMemberDetails (one day) returns empty rdd when no files") {

    val date = "2017/12/01"
    val res = getMemberDetails(date, 18).collect()

    res.toList should equal(List())
  }

  test("getMemberDetails (one day) returns empty rdd when file is empty") {

    val date = "2017/12/05"
    val res = getMemberDetails(date,17)

    res.collect().toList should equal(List())
  }

  test("getMemberDetails (one day) throws exception when date is in wrong format") {

    val date = "2017-12-01"

    val thrown = intercept[Exception]{
      getMemberDetails(date,17)
    }

    thrown.getMessage should equal(s"requirement failed: date = $date in path = $basePath/$date/17/* does not have the correct format: YYYY/MM/DD.")
  }

  test("getMemberDetails (one day) throws exception if spark can't read files") {
    val date = "2017/12/01"
    val path = s"$basePath/$date/17/*"
    val mockSparkContext = mock[SparkContext]

    val thrown = intercept[IllegalStateException]{
      doThrow(new IllegalStateException("Bla bla")).when(mockSparkContext).textFile(path)
      implicit val sc = mockSparkContext
      getMemberDetails(date,17)
    }

    thrown.getMessage should equal("Bla bla")
  }

  test("getMemberDetails (multiple days) returns empty rdd when no days given") {

    val res = getMemberDetails(List(),17)

    res.collect().toList should equal(List())
  }

  test("getMemberDetails (multiple days) can read adds and removals") {

    val dates = List("2017/12/01","2017/12/02","2017/12/03","2017/12/04")
    val res = getMemberDetails(dates,17)

    val expectedResult = List(
      MemberDetails("001","2017/12/01",Addition),
      MemberDetails("002","2017/12/01",Addition),
      MemberDetails("003","2017/12/01",Addition),
      MemberDetails("004","2017/12/01",Removal),
      MemberDetails("005","2017/12/01",Removal),
      MemberDetails("006","2017/12/01",Removal),
      MemberDetails("007","2017/12/01",Removal),
      MemberDetails("003","2017/12/02",Removal),
      MemberDetails("008","2017/12/02",Removal),
      MemberDetails("009","2017/12/02",Removal),
      MemberDetails("009","2017/12/03",Addition),
      MemberDetails("010","2017/12/04",Addition),
      MemberDetails("010","2017/12/04",Removal))

    res.collect().toList.sortBy(_.memberNo) should equal(expectedResult.sortBy(_.memberNo))
  }

  test("getMembers returns empty rdd when files are empty or doesn't exists") {

    val date = List("2017/12/05","2017/12/06")
    val res = getMembers(date,17)

    res.collect().toList should equal(List())
  }

  test("getMembers can find all added members that are not removed") {

    val dates = List("2017/12/01","2017/12/02","2017/12/03","2017/12/04")
    val res = getMembers(dates,17)
    val expectedResult = List("001","002","009","010")

    res.collect().toList.sorted should equal(expectedResult)
  }
}
