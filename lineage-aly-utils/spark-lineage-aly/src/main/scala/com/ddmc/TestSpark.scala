package com.ddmc

import com.ddmc.api.LineageData
import com.ddmc.graph.Load2neo4j
import org.apache.spark.sql.SparkSession

object TestSpark {

  private val NEO4J_BOLT_URL = "bolt://localhost:7687"
  private val NEO4J_USERNAME = "neo4j"
  private val NEO4J_PASSWORD = "365530302"

  def main(args: Array[String]): Unit = {
      val sparkSession = SparkSession.builder()
        .config("hive.metastore.uris", "thrift://localhost:9083")
        .config("spark.sql.queryExecutionListeners","com.ddmc.FlQueryExecutionListener" )
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    val sql =
      """
        |create table if not exists dwd.student4 as
        |select student_name2,count(student_name) as cnt
        |      from
        |      (select concat(student_name,'-',b.product_Id) as student_name2,a.*
        |      from
        |      (select
        |      if(id='1',id,concat(student_id,'-',name)) as student_name
        |      ,c.store_name
        |      ,b.score
        |      ,id,name
        |      from student a
        |      left join score b
        |       ON a.id = b.student_id
        |      inner join dim.store c
        |       ON a.id = c.store_id
        |       and c.snapshot = '2023-04-25'
        |      where name='chenbiao'
        |      ) a
        |      left join dim.product_hive b
        |       ON a.id = b.product_id
        |      ) t
        |      group by 1
        |""".stripMargin

    sparkSession.sql(sql)

//    val linage = new SparkLinage(sparkSession)
//    val data: LineageData = linage.getLineAgeData(sql)
//    val load2neo4j = new Load2neo4j(NEO4J_BOLT_URL, NEO4J_USERNAME, NEO4J_PASSWORD)
//    load2neo4j.loadDataFromSql(data)
//    println(data)
  }

}
