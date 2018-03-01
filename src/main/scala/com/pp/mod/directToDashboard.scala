package com.pp.mod

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object directToDashboard {

  val conf = ConfigFactory.load

  def createDbConfig(dbIdentifier: String): (String,String,String) = {
    val userDB: String = conf.getString(s"$dbIdentifier.db.username")
    val passwordDB: String = conf.getString(s"$dbIdentifier.db.password")
    val jdbcDriverDB:String = conf.getString(s"$dbIdentifier.db.driver")
    (userDB, passwordDB, jdbcDriverDB)
  }

  def createSpark(): SparkSession = {
    val spark = SparkSession.builder().appName("directToDashboard").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def readOracle(spark: SparkSession): List[Future[String]] = {
    val jdbcDB: String = s"""jdbc:oracle:thin:@//${conf.getString(s"fisicien.db.ip")}/${conf.getString(s"fisicien.db.sid")}"""
    val tableListDB: List[String] = conf.getStringList(s"fisicien.db.tables").asScala.toList.map(x=> x.toUpperCase())
    val (userDB, passwordDB, jdbcDriverDB) = createDbConfig("fisicien")
    val prop = new java.util.Properties
    prop.setProperty("driver", jdbcDriverDB)
    prop.setProperty("user", userDB)
    prop.setProperty("password", passwordDB)

    val list_future_tables: List[Future[String]] =
    tableListDB.map(table => {
      val future_read  = Future {spark.read.jdbc(jdbcDB, table, prop)}
      //println("!!!"+Thread.activeCount)
      val future_write: Future[String] = future_read.map(df => {
        val (schema_name: String, table_name: String) = (table.split('.')(0), table.split('.')(1))
        writeHdfs(df, schema_name, table_name, spark)
      }
      )

      val future_tempTables: Future[String] = future_write.map(table_name => {
        registerTables(table_name, spark)
      })

      future_tempTables onComplete   {
        case Failure(t) => {
          println(s"ERROR: Skipping table $table")
          t.printStackTrace
        }
        case Success(s) => {
          println(s"INFO: Created temp view for table   ${table.toUpperCase} ")
        }
      }
      //Await.result(future_tempTables, 24 hours)
      future_tempTables
    })
    list_future_tables
  }

  def readMsSql: Unit = {

  }

  def readMySql: Unit = {

  }

  def writeHdfs(df: sql.DataFrame, schema_name: String, table_name: String, spark: SparkSession): String = {
    val loc = conf.getString("hdfs.location")
    println("!!! no of threads "+Thread.activeCount)
    println("!!! no of cores "+Runtime.getRuntime().availableProcessors())
    println(s"INFO: Writing to hdfs for table $schema_name $table_name")
    //df.withColumn("schema_name", sql.functions.lit(s"$schema_name")).write.mode("ignore").parquet(s"$loc$table_name")
    df.withColumn("schema_name", sql.functions.lit(s"$schema_name")).write.mode("append").parquet(s"$loc$table_name")
    table_name
  }

  def registerTables(table_name: String, spark: SparkSession): String = {
    val loc = conf.getString("hdfs.location")
    val df = spark.read.parquet(s"$loc$table_name")
    df.createOrReplaceTempView(table_name)
    table_name
  }

  def agg_medical_tourism(spark: SparkSession): (sql.DataFrame, sql.DataFrame) = {
    // edits made:
    // changed :: to cast(xx as yy)
    // changed || to concat
    // changed text to string
    // changed mod_fin_dafaultdb_prod_env to <nothin>
    // replaced admission_date to admission_datetime
    val df_census: sql.DataFrame =
    spark.sql(
      """
        |select ACC_period,hospital_id,NATIONALITY,patient_type,ADMIT_STATUS,count(distinct episode_seq) as episode_count
        |from (
        |SELECT concat(substring(cast(epi.admission_datetime as string),1,4) , '0' , substring(cast(epi.admission_datetime as string),6,2)) as  ACC_period,
        |epi.schema_name as hospital_id,NAT.code_description AS NATIONALITY,
        |    case when epi.patient_type='1' then 'INPATIENT' when epi.patient_type='2' then 'OUTPATIENT' when epi.patient_type='3' then 'DAYCARE' when epi.patient_type='4' then 'EMERGENCY' else epi.patient_type end as patient_type,
        |EPI.ADMIT_STATUS,epi.episode_no  as  episode_seq
        |FROM (select * from FDEPISODE where ADMIT_STATUS NOT IN ('2','5','6','7')) EPI
        |LEFT JOIN FDPATIENT PAT ON EPI.fdpatient_id=PAT.fdpatient_id and EPI.schema_name = PAT.schema_name
        |LEFT JOIN FDPERSON PER ON PAT.fdperson_id=PER.fdperson_id and PAT.schema_name = PER.schema_name
        |LEFT JOIN VW_NAT NAT ON PER.nationality=NAT.code and PER.schema_name = nat.schema_name
        |LEFT JOIN GNCAREPROVIDER GCP ON GCP.GNCAREPROVIDER_ID = EPI.ATTEND_GNCAREPROVIDER_ID and GCP.schema_name = EPI.schema_name
        |INNER JOIN (select distinct fdperson_id,schema_name from FDADDRESS where country !='MYS' or trim(country) is null) AD ON PAT.fdperson_id=AD.fdperson_id  and PAT.schema_name=AD.schema_name
        |WHERE (per.nationality !='MAL' or trim(per.nationality) is null)
        |AND GCP.CAREPROVIDER_CODE NOT IN ('DOTICON','DPMCRS','DPS')
        |) as rpt_census_medical_tourism_report
        |group by 1,2,3,4,5
      """.stripMargin)
    val df_revenue: sql.DataFrame =
      spark.sql(
        """
          |select ACC_period,hospital_id,NATIONALITY,patient_type,ADMIT_STATUS,sum(BILL_SIZE) as revenue
          |from (
          |SELECT concat(substring(cast(epi.admission_datetime as string),1,4),'0',substring(cast(epi.admission_datetime as string),6,2)) as ACC_period,
          |epi.schema_name as hospital_id,NAT.code_description AS NATIONALITY,
          |    case when epi.patient_type='1' then 'INPATIENT' when epi.patient_type='2' then 'OUTPATIENT' when epi.patient_type='3' then 'DAYCARE' when epi.patient_type='4' then 'EMERGENCY' else epi.patient_type end as patient_type,
          |EPI.ADMIT_STATUS,BOEPI.BILL_SIZE
          |FROM (select * from FDEPISODE where ADMIT_STATUS IN ('4','IC') )EPI
          |LEFT JOIN BOEPISODEACCOUNT BOEPI ON EPI.episode_no=BOEPI.episode_no and EPI.fdepisode_id=BOEPI.fdepisode_id and EPI.schema_name=BOEPI.schema_name
          |LEFT JOIN FDPATIENT PAT ON EPI.fdpatient_id=PAT.fdpatient_id  and EPI.schema_name=PAT.schema_name
          |LEFT JOIN FDPERSON PER ON PAT.fdperson_id=PER.fdperson_id and PAT.schema_name=PER.schema_name
          |LEFT JOIN VW_NAT NAT ON PER.nationality=NAT.code and PER.schema_name = nat.schema_name
          |INNER JOIN (select distinct fdperson_id,schema_name from FDADDRESS where country !='MYS' or trim(country) is null) AD
          |ON PAT.fdperson_id=AD.fdperson_id  and PAT.schema_name=AD.schema_name
          |where PER.nationality != 'MAL' or trim(PER.nationality) is  null
          |) as rpt_revenue_medical_tourism_report
          |group by 1,2,3,4,5
        """.stripMargin)
    //df.show
    //println(df.filter(col("ACC_period") === lit("2015007")).show)
    (df_census, df_revenue)
  }

  def writePostgre(df: sql.DataFrame, table: String, spark: SparkSession): String = {
    /*
        CREATE TABLE defaultdb.d2d_db.rpt_census_medical_tourism_report_aggr as select * from defaultdb.mod_fin_dafaultdb_prod_env.rpt_census_medical_tourism_report_aggr limit 0;
        select * from defaultdb.d2d_db.rpt_census_medical_tourism_report_aggr;
     */
    val jdbcDB: String = s"""jdbc:postgresql://${conf.getString(s"datamart.db.ip")}/${conf.getString(s"datamart.db.database")}"""
    val (userDB, passwordDB, jdbcDriverDB) = createDbConfig("datamart")
    val prop = new java.util.Properties
    prop.setProperty("driver", jdbcDriverDB)
    prop.setProperty("user", userDB)
    prop.setProperty("password", passwordDB)
    println(s"INFO: Writing to datamart for table ${table.toUpperCase}")
    println("<mocking write>")
    //df.write.mode("overwrite").jdbc(jdbcDB, table, prop)
    /*val future_write_postgre: Future[String] = Future {
      df.write.mode("overwrite").jdbc(jdbcDB, table, prop)
      table
    }
    Await.result(future_write_postgre, 24 hours)
    future_write_postgre*/
    table
  }


  def main(args: Array[String]): Unit = {
    //create spark session
    val spark = createSpark
    //read oracle tables and write to hdfs as parquet. include schema name
    val list_future_tables_oracle: List[Future[String]] = readOracle(spark)
    val seq_futures_oracle = Future.sequence(list_future_tables_oracle)
    Await.result(seq_futures_oracle, 24 hours)
    //MEDICAL TOURISM
    val future_agg_medical_tourism: Future[(sql.DataFrame,sql.DataFrame)] = Future { agg_medical_tourism(spark) }
    //val seq_future_postgres = ListBuffer.empty[Future[String]]
    val future_agg_medical_tourism_write_to_postgre: Future[String] = future_agg_medical_tourism.map(dfs => {
      writePostgre(dfs._1, "defaultdb.d2d_db.rpt_census_medical_tourism_report_aggr", spark)
      writePostgre(dfs._2, "defaultdb.d2d_db.rpt_revenue_medical_tourism_report_aggr", spark)
    })
    /*future_agg_medical_tourism_write_to_postgre.map(x => {
      val seq_future = Future.sequence(x)
      Await.result(seq_future, 24 hours)

    })*/
    Await.result(future_agg_medical_tourism_write_to_postgre, 24 hours)
  }
}
