import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


case class Part(p_partkey:Int, p_name:String, p_mfgr:String, p_brand:String, p_type:String, p_size:Int, p_container:String, p_retailprice:Double, p_comment:String)
case class Lineitem(l_orderkey:Int, l_partkey:Int, l_suppkey:Int, l_linenumber:Int, l_quantity:Double, l_extendedprice:Double, l_discount:Double, l_tax:Double, l_returnflag:String, l_linestatus:String, l_shipdate:String, l_commitdate:String, l_receiptdate:String, l_shipinstruct:String, l_shipmode:String, l_comment:String)
case class Orders(o_orderkey:Int, o_custkey:Int, o_orderstatus:String, o_totalprice:Double, o_orderdate:String,o_year:String, o_orderpriority:String, o_clerk:String, o_shippriority:Int, o_comment:String)
case class Supplier(s_suppkey:Int, s_name:String, s_address:String, s_nationkey:Int, s_phone:String, s_acctbal:Double, s_comment:String)
case class Partsupp(ps_partkey:Int, ps_suppkey:Int, ps_availqty:Int, ps_supplycost:Double, ps_comment:String)
case class Nation(n_nationkey:Int, n_name:String, n_regionkey:Int, n_comment:String)
case class Customer(c_custkey:Int, c_name:String, c_address:String, c_nationkey:Int, c_phone:String, c_acctbal:Double, c_mktsegment:String, c_comment:String)
case class Region(r_regionkey:Int, r_name:String, r_comment:String)
//case class q9_product_type_profit1(nation:String, o_year:String, sum_profit: Double)

object SimpleApp {

  def main(args: Array[String]) = {



      if(args.length < 2) {
        println("two arguments:data directory and sql script")
      }

      /*val source = scala.io.Source.fromFile(args(1))
      val sqlLines = try source.mkString finally source.close()*/


    val dir=args(0)
    val sparkConf = new SparkConf().setAppName("q5")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)




    val sqlScript:String = sc.textFile(args(1)).collect().reduce((a,b)=>a+b)

    import sqlContext.implicits._
    val part = sc.textFile(dir + "/part.tbl").map(_.split("\\|")).map(p => Part(p(0).trim.toInt, p(1), p(2), p(3), p(4), p(5).trim.toInt, p(6), p(7).trim.toDouble, p(8))).cache()
    val lineitem = sc.textFile(dir+"/lineitem.tbl").map(_.split("\\|")).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15)))
    val orders = sc.textFile(dir+"/orders.tbl").map(_.split("\\|")).map(p => Orders(p(0).trim.toInt, p(1).trim.toInt, p(2), p(3).trim.toDouble, p(4),p(4).split("-")(0), p(5), p(6), p(7).trim.toInt, p(8)))
    val supplier = sc.textFile(dir+"/supplier.tbl").map(_.split("\\|")).map(p => Supplier(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt, p(4), p(5).trim.toDouble, p(6)))
    val partsupp = sc.textFile(dir+"/partsupp.tbl").map(_.split("\\|")).map(p => Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4)))
    val nation = sc.textFile(dir+"/nation.tbl").map(_.split("\\|")).map(p => Nation(p(0).trim.toInt, p(1), p(2).trim.toInt, p(3)))
    val customer = sc.textFile(dir+"/customer.tbl").map(_.split("\\|")).map(p => Customer(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt, p(4), p(5).trim.toDouble, p(6), p(7)))
    val region = sc.textFile(dir+"/region.tbl").map(_.split("\\|")).map(p => Region(p(0).trim.toInt, p(1), p(2)))

    part.toDF().registerTempTable("part")
    lineitem.toDF().registerTempTable("lineitem")
    orders.toDF().registerTempTable("orders")
    supplier.toDF().registerTempTable("supplier")
    partsupp.toDF().registerTempTable("partsupp")
    nation.toDF().registerTempTable("nation")
    customer.toDF().registerTempTable("customer")
    region.toDF().registerTempTable("region")


    val result = sqlContext.sql(sqlScript)

/*    val result = sqlContext.sql("select n_name,sum(l_extendedprice * (1 - l_discount)) " +
      "as revenue from customer,orders,lineitem,supplier,nation,region " +
      "where c_custkey = o_custkey " +     //custom ,order,lin,sup,nat,region
      "and l_orderkey = o_orderkey " +
//      "and l_suppkey = s_suppkey " +
   //   "and c_nationkey = s_nationkey " +
 //     "and s_nationkey = n_nationkey " +
      "and n_regionkey = r_regionkey " +
      "and r_name = 'AMERICA' " +
      "and o_orderdate >= '1994-01-01' " +
      "and o_orderdate < '1995-01-01'  group by n_name")*/

    result.collect.foreach {
      p => println(p)
    }
   // println(sqlScript.reduce((a,b)=>a+b))
 //q5 opt
/*    val result = sqlContext.sql("select n_name,sum(l_extendedprice * (1 - l_discount)) " +
  " as revenue from customer,orders,lineitem,supplier,nation,region " +
  "where c_custkey = o_custkey " +
  "and c_nationkey = s_nationkey " +
  "and s_nationkey = n_nationkey " +
  "and n_regionkey = r_regionkey " +
  "and r_name = 'AMERICA' " +
  "and o_orderdate >= '1994-01-01' " +
  "and o_orderdate < '1995-01-01' " +
  "and l_orderkey = o_orderkey " +
  "and l_suppkey = s_suppkey group by n_name")

    val s1 = sqlContext.sql("select s_suppkey, n_name from nation n join supplier s on n.n_nationkey = s.s_nationkey" )
    val l1_tmp = sqlContext.sql("select l_suppkey, l_extendedprice, l_discount, l_quantity, l_partkey, l_orderkey, n_name from" +
      "s1")
    val l1 = l1_tmp.join(lineitem.toSchemaRDD,Inner,on=Some("s_suppkey = l_suppkey"))
    println(l1.collect())

    val st = "1990-12-01"
    val result = st.split("-")(0)



      println(result.toDebugString)
      result.collect.foreach{
        p=>println(p)
      }*/
  }
}
