
import org.apache.spark.sql.{DataFrame,Dataset,Row,SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RunIntro extends Serializable {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("Intro")
            .master("local")
            .getOrCreate()
        
        import spark.implicits._
        val dfSchema = StructType(Array(
                StructField("id_1",IntegerType,true),
                StructField("id_2",IntegerType,true),
                StructField("cmp_fname_c1",DoubleType,true),
                StructField("cmp_fname_c2",DoubleType,true),
                StructField("cmp_lname_c1",DoubleType,true),
                StructField("cmp_lname_c2",DoubleType,true),
                StructField("cmp_sex",IntegerType,true),
                StructField("cmp_bd",IntegerType,true),
                StructField("cmp_bm",IntegerType,true),
                StructField("cmp_by",IntegerType,true),
                StructField("cmp_plz",IntegerType,true),
                StructField("is_match",BooleanType,true)
        ))
        val df = spark.read
            .option("header","true")
            .option("nullValue","?")
            .schema(dfSchema)
            .csv("/Users/soda/Downloads/dataSet/aas/linkage/*.csv")
        df.createOrReplaceTempView("matchdata")
        val result = spark.sql("""
                select cmp_lname_c1 + cmp_plz + cmp_by + cmp_bd + cmp_bm as score,
                    is_match from matchdata
        """)
        def crossTab(scored: DataFrame, t: Double): DataFrame = {
            scored.selectExpr(s"score >= $t as above","is_match")
                .groupBy("above")
                .pivot("is_match",Seq("true","false"))
                .count()
        }
        
        crossTab(result,4.0).show()
    }
}
