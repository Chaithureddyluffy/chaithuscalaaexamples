import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, current_date, date_format, datediff, initcap, sum, when}

object CodingProblems {
  def main(arr:Array[String]): Unit = {

    val conf=new SparkConf()
    conf.set("spark.appname","chaithu")
    conf.set("spark.master","local[4]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    /*Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
      based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
      50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
    sales aggregated by performance status.*/
   /* val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)
    ).toDF("name", "total_sales")
    val df=sales.select(
      initcap(col("name")).alias("name"),
      col("total_sales"),
      when(col("total_sales")<25000,"NEED IMPROVEMENT").
        when((col("total_sales")>25000)&&(col("total_sales")<=50000),"GOOD").otherwise("Excellent").alias("status")
    )
    val x=df.groupBy(col("status")).agg(sum("total_sales")).show()*/

    /*val workload = List(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
    ("neha", "ProjectC", 80),
    ("neha", "ProjectD", 30),
    ("priya", "ProjectE", 110),
    ("mohan", "ProjectF", 40),
    ("ajay", "ProjectG", 70),
    ("vijay", "ProjectH", 150),
    ("veer", "ProjectI", 190),
    ("aatish", "ProjectJ", 60),
    ("animesh", "ProjectK", 95),
    ("nishad", "ProjectL", 210),
    ("varun", "ProjectM", 50),
    ("aadil", "ProjectN", 90)
    ).toDF("name", "project", "hours")
    val x=workload.select(
      initcap(col("name")).alias("name"),
      when(col("hours")<100,"Underutilized").when((col("hours")>100)&&(col("hours")<200),"Balanced").otherwise("Overload").alias("work")
    )
    val y=x.groupBy(col("work")).agg(count("work")).show()*/

    /*val employees = List(
      ("karthik", 62),
      ("neha", 50),
      ("priya", 30),
      ("mohan", 65),
      ("ajay", 40),
      ("vijay", 47),
      ("veer", 55),
      ("aatish", 30),
      ("animesh", 75),
      ("nishad", 60)
    ).toDF("name", "hours_worked")
    val a=employees.select(
      initcap(col("name")).alias("name"),
      col("hours_worked"),
      when(col("hours_worked")<45
        ,"noovertime").when((col("hours_worked")>45)
        &&(col("hours_worked")<=60),"Balanced").otherwise("overtime").alias("time")
    )
    val b=a.groupBy(col("time")).agg(count("time")).show()*/
   /* val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")
    val a=customers.select(
      initcap(col("name")).alias("name"),
      col("age"),
      when(col("age")<25,"youth")
        .when((col("age")>25)&&(col("age")<=45),"adult")
        .otherwise("senior").alias("ages")
    )
    val b=a.groupBy("ages").agg(count("ages")).show()*/
    /*val vehicles = List(
      ("CarA", 30),
      ("CarB", 22),
      ("CarC", 18),
      ("CarD", 15),
      ("CarE", 10),
      ("CarF", 28),
      ("CarG", 12),
      ("CarH", 35),
      ("CarI", 25),
      ("CarJ", 16)
    ).toDF("vehicle_name", "milaeage")
    val a=vehicles.select(
      col("vehicle_name"),
      col("milaeage"),
      when(col("milaeage")<15,"Low Efficiency").when((col("milaeage")>15)
        &&(col("milaeage")<25),"Moderate Efficiency").otherwise("High Efficiency").alias("carmillage")
    ).show()*/
    /*val students = List(
      ("karthik", 95),
      ("neha", 82),
      ("priya", 74),
      ("mohan", 91),
      ("ajay", 67),
      ("vijay", 80),
      ("veer", 85),
      ("aatish", 72),
      ("animesh", 90),
      ("nishad", 60)
    ).toDF("name", "score")
    val a=students.select(
      col("name"),
      col("score"),
      when(col("score")<75,"Needs Improvement").
        when((col("score")>75)&&(col("score")<=89)
          ,"Good").otherwise("Excellent").alias("category")
    )
    val b=a.groupBy(col("category")).agg(count("category")).show()*/
    /*val inventory = List(
      ("ProductA", 120),
      ("ProductB", 95),
      ("ProductC", 45),
      ("ProductD", 200),
      ("ProductE", 75),
      ("ProductF", 30),
      ("ProductG", 85),
      ("ProductH", 100),
      ("ProductI", 60),
      ("ProductJ", 20)
    ).toDF("product_name", "stock_quantity")
    val b=inventory.select(
      col("product_name"),
      col("stock_quantity"),
      when(col("stock_quantity")<50,"Low Stock").
        when((col("stock_quantity")>50)
          &&(col("stock_quantity")<=100),"Normal").otherwise("Overstocked").alias("category")
    )
    val a=b.groupBy(col("category")).agg(count("stock_quantity"),sum("stock_quantity")).show*/
    /*val employees = List(
      ("karthik", "Sales", 85),
      ("neha", "Marketing", 78),
      ("priya", "IT", 90),
      ("mohan", "Finance", 65),
      ("ajay", "Sales", 55),
      ("vijay", "Marketing", 82),
      ("veer", "HR", 72),
      ("aatish", "Sales", 88),
      ("animesh", "Finance", 95),
      ("nishad", "IT", 60)
    ).toDF("name", "department", "performance_score")
    val a=employees.select(
      initcap(col("name")).alias("name"),
      col("department"),
      col("performance_score")
    ).filter(col("department").isin("Sales","Marketing"))
    val b=a.withColumn("performance",
      when(col("performance_score")>=85,col("performance_score")*0.20)
        .when((col("performance_score")>50)&&(col("performance_score")<85),col("performance_score")*0.15).otherwise(0)
    ).show()*/
   /*val products = List(
      ("Laptop", "Electronics", 120, 45),
      ("Smartphone", "Electronics", 80, 60),
      ("Tablet", "Electronics", 50, 72),
      ("Headphones", "Accessories", 110, 47),
      ("Shoes", "Clothing", 90, 55),
      ("Jacket", "Clothing", 30, 80),
      ("TV", "Electronics", 150, 40),
      ("Watch", "Accessories", 60, 65),
      ("Pants", "Clothing", 25, 75),
      ("Camera", "Electronics", 95, 58)
    ).toDF("product_name", "category", "return_count", "satisfaction_score")
    val c=products.select(
      col("product_name"),
      col("category"),
      col("return_count"),
      col("satisfaction_score"),
        when((col("return_count")>50)&&(col("return_count")<100)&&(col("satisfaction_score")>50)&&(col("satisfaction_score")<70),"Moderate")
          .when((col("return_count")>100)&&(col("satisfaction_score")<50),"High Return Rate")
          .otherwise("low").alias("result")
    ).show()*/
    //val d=c.groupBy("category").agg(count("category")).orderBy("result").show()

    /*val customers = List(
      ("karthik", "Premium", 1050, 32),
      ("neha", "Standard", 800, 28),
      ("priya", "Premium", 1200, 40),
      ("mohan", "Basic", 300, 35),
      ("ajay", "Standard", 700, 25),
      ("vijay", "Premium", 500, 45),
      ("veer", "Basic", 450, 33),
      ("aatish", "Standard", 600, 29),
      ("animesh", "Premium", 1500, 60),
      ("nishad", "Basic", 200, 21)
    ).toDF("name", "membership", "spending", "age")
    val a=customers.withColumn("customer",
      when(col("spending")>1000,"highspending")
        .when((col("spending")>500)&&(col("spending")<1000)
          ,"AverageSpender").otherwise("Lowspender")


    )
   val c=a.groupBy(col("customer")).agg(count("customer")).show()*/
    /*val orders = List(
      ("Order1", "Laptop", "Domestic", 2),
      ("Order2", "Shoes", "International", 8),
      ("Order3", "Smartphone", "Domestic", 3),
      ("Order4", "Tablet", "International", 5),
      ("Order5", "Watch", "Domestic", 7),
      ("Order6", "Headphones", "International", 10),
      ("Order7", "Camera", "Domestic", 1),
      ("Order8", "Shoes", "International", 9),
      ("Order9", "Laptop", "Domestic", 6),
      ("Order10", "Tablet", "International", 4)
    ).toDF("order_id", "product_type", "origin", "delivery_days")
    val c=orders.withColumn("Delayes",
      when(col("delivery_days")<3,"Fast").when((col("delivery_days")>3)&&(col("delivery_days")<=7),"Ontime").otherwise("International")
    )
    val d=c.groupBy(col("Delayes")).agg(count("Delayes")).show()*/
    /*val employees = List(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")
    ).toDF("name", "last_checkin")
    val c=employees.withColumn("activestatus",
      when(datediff(current_date(),col("last_checkin"))<=7,"Active").otherwise("INACTIVE")
    ).show()*/

  }

}
