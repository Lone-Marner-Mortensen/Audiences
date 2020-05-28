package audience

import scala.util.{Try, _}
import org.apache.hadoop.mapred.InvalidInputException
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

trait MembersAddOrRemove
case object Addition extends MembersAddOrRemove
case object Removal extends MembersAddOrRemove

object Members {

  case class MemberDetails(memberNo:String,date:String,addOrRemove:MembersAddOrRemove)

  private[audience] def getMemberDetails(date:String,audienceId:Int)(implicit basePath:String, sc:SparkContext): RDD[MemberDetails] = {

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd")
    dateFormat.setLenient(false)

    require(Try(dateFormat.parse(date)).isSuccess,s"date = $date in path = $basePath/$date/$audienceId/* does not have the correct format: YYYY/MM/DD.")

    val path = s"$basePath/$date/$audienceId/*"
    val addOrRemove = if(audienceId < 0)Removal else Addition
    val members: RDD[MemberDetails] = sc.textFile(path).map(MemberDetails(_,date,addOrRemove))//Won't fail because of sparks lazy evaluation.

     Try(members.isEmpty) match {

      case Failure(e) => if(e.isInstanceOf[InvalidInputException])
                            sc.emptyRDD[MemberDetails]//Should return empty rdd, if there are no files for the given day.
                         else
                            throw e
      case Success(_) => members
    }
  }

  private[audience] def getMemberDetails(dates:List[String],audienceId:Int)(implicit basePath:String, sc:SparkContext): RDD[MemberDetails] = {

    val rdds: Seq[RDD[MemberDetails]] = dates.map{ d => getMemberDetails(d,audienceId).union(getMemberDetails(d,(-1)*audienceId)) }
    sc.union(rdds)
  }

  //Get all the additions (removals are subtracted).
  def getMembers(dates:List[String],audienceId:Int)(implicit basePath:String, sc:SparkContext): RDD[String] = {

    val memberDetails = getMemberDetails(dates,audienceId)

    if (memberDetails.isEmpty)
      sc.emptyRDD[String]
    else {
      memberDetails
        .groupBy { md => md.memberNo }
        .flatMap { case (_, mdIterator) =>

          val md = mdIterator.toList
          val mdLastDay = md.maxBy(_.date)

          //Addition has precedence over Removal.
          val containsAddition = md.exists{ m => m.date == mdLastDay.date && m.addOrRemove == Addition }

          if (containsAddition)
            Some(mdLastDay.memberNo)
          else
            None
        }
    }
  }
}
