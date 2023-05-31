package com.ddmc

import com.ddmc.api.LineageData
import com.ddmc.api.LineageData.EdgeInfo
import com.google.common.hash.Hashing
import org.apache.spark.sql.SparkSession

import java.nio.charset.Charset

class SparkLinage(sparkSession: SparkSession) extends  com.ddmc.api.LineAgeAly {

  private val listenV3 = new FlQueryExecutionListener()

  override def getLineAgeData(sql: String): LineageData = {
    val start = System.currentTimeMillis()
    val parser = sparkSession.sessionState.sqlParser
    val analyzer = sparkSession.sessionState.analyzer
    val optimizer = sparkSession.sessionState.optimizer
    //    val planner = sparkSession.sessionState.planner

    val newPlan = parser.parsePlan(sql)

    val analyzedPlan = analyzer.executeAndCheck(newPlan)

    val optimizerPlan = optimizer.execute(analyzedPlan)
    //    //得到sparkPlan
    //    val sparkPlan = planner.plan(optimizerPlan).next()

    println(optimizerPlan)
    val lineageMap = listenV3.lineageParserPlan(optimizerPlan)


    val lineageData = new LineageData
    lineageData.setQueryText(sql);
    lineageData.setEngine("spark")
    lineageData.setVersion(sparkSession.version)
    lineageData.setTimestamp(start)
    lineageData.setHash(getQueryHash(sql))
    lineageMap.keySet.foreach(key=>{
      val info = new EdgeInfo()
      info.addTarget(key)
      info.setEdgeType(LineageData.EdgeType.PROJECT)
      //TODO 待完善表达式
      info.setExpression("")
      lineageData.addColumn(key)
      lineageMap(key).foreach(column=>{
        info.addSource(column)
        lineageData.addColumn(column)
      })
      lineageData.getEdges.add(info)


    })
    val end = System.currentTimeMillis()
    lineageData.setDuration(end-start)

    lineageData
  }

  private def getQueryHash(queryStr: String) = {
    val hasher = Hashing.md5.newHasher
    hasher.putBytes(queryStr.getBytes(Charset.defaultCharset))
    hasher.hash.toString
  }
}

