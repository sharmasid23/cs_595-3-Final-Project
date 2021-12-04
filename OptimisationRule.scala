import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable
import org.apache.spark.sql.DataFrame


import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression

import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util._
import spark.implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.expressions._
import org.apache.spark.util.random.RandomSampler

val df1 = Seq(
    (Some(1), Some("2021-04-1"), Some("AI"), Some("NYC")),
    (Some(2), Some("2021-04-4"), Some("UNT"), Some("TXA"))
).toDF("flightid", "departdate" , "companyname", "goingto")

df1.write.format("orc").mode("overwrite").saveAsTable("df1table")

object ProjectOptimizationRule extends Rule[LogicalPlan] 
{
    def apply(plan: LogicalPlan): LogicalPlan = 
    {
        val rootIC = Set(plan.output) 
        val stringIC = rootIC.map(_.name)
        val nextIC = oneStep(plan, stringIC) 
        plan.setTagValue(TreeNodeTag[Set[Attribute]]("icols"), nextIC)
    }
}
def oneStep(op: LogicalPlan, curIcols: Set[String]): Set[String] = 
{
    op match 
    {
        case f: Filter (conditions, child) => 
        {
            val condattribute = conditions.references{}
            val stringcondatrr = condattribute.map(_.name)
            val newresult = curIcols ++ stringcondatrr
            newresult
        }
        case p: Project (projectList, child) =>
        {
            val filList= projectList.filter(curIcols.contains(_.name))
            var neededIcols:Set[Attribute] = Set()
            for( exp <- filList)
                neededIcols= neededIcols ++ exp.references{}  

            val neededIcolsString= needeedIcols.map(._name)
            val newresult1= curIcols ++ neededIcolsString
            newresult1
        }
        case j: Join ( left: LogicalPlan, right: LogicalPlan, joinType: LeftOuter, condition: Option[Expression])
            val cattr= 
            val l = left.output 
            val r =right.outplan.map(_.withNullability(true))
            val newresult3= l ++ r
    }
}

spark.experimental.extraOptimizations =  Seq(ProjectOptimizationRule)
spark.sql("select df1.flightid, (df1.flightid+df1.flightnumber) AS v from df1 where df1.flightid > 2").collect()






//newresult
                //df.select(col(*), when(col("companytname") == "?")).show()
            
            // case p: Project (projectList, child)=>
            // val expression = expression.references{}
            // projectList.filter(x => curIcols.contains(x) )














//  val df4 = Seq(
//   (Some(1), Some("x"), Some("a"), Some(10)),
//   (Some(2), Some("y"), Some("b"), Some(20)),
//   (Some(3), Some("z"), Some("c"), Some(30))
// ).toDF("c1", "c2", "c3", "c4")
// // val cols1 = List("flightid", "departdate" , "companyname", "goingto")
// // val df1 = spark.createDataFrame(data1).toDF(cols1:_*)

// val data2 = Seq
// ((10, "2021-04-3", "AI", "LA"),
// (11, "2021-04-1", "UTD", "TXA"),
// (12, "2021-04-1", "KLM", "NYC"))
// .toDF("flightid", "arrivaldate" , "companyname", "comingfrom")
// // val cols2 = List("flightid", "arrivaldate" , "companyname", "comingfrom")
// // val df2 = spark.createDataFrame(data2).toDF(cols2:_*)












// val ic = ...
// val op
// op.setTagValue(TreeNodeTag("icols"), ic)
// object ComputeICols extends Rule[LogicalPlan] {
// def apply(plan: LogicalPlan): LogicalPlan = {
// // if top of plan need all attributes
// val rootIC = Set(plan.output) // { g, state_id }
// val nextIC = oneStep(plan, rootIC) // { city, state_id }
// 1
// plan.setTagValue(TreeNodeTag[Set[Attribute]]("icols"), nextIC)
// }
// def oneStep(op: LogicalPlan, curIcols: Set[Attribute]): Set[Attribute] = {
// op match {
// case Filter(c, child) => {
// // analyze condition c
// // add attribtues from c to curIcols
// // return updated icols
// }
// case Project(projExprs, child) => {
// // find the projection expressions that generate the attributes we have
// ,→ in curIcols
// // extract all attributes from them and union them as new icols
// // return updated icols
// }
// case _ => {
// Set[Attribute]()
// }
// }
// }
// }
// public class F