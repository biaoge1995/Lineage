package com.ddmc


import com.ddmc.api.LineageData
import com.ddmc.api.LineageData.Column
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * auth:chenbiao
 * 手动获取解析计划为 explain extended
 * 该方法为通过解析逻辑执行计划获取字段间的血缘关系
 *
 */

case class TargetTableColumnId(table:String ,id:Long)

class FlQueryExecutionListener extends QueryExecutionListener with Logging {


  // 目标表可能有多张
  private val targetTableColumns: mutable.Map[TargetTableColumnId, Column] = mutable.Map()
  // source表 可能有多个
  private val sourceTableColumns: mutable.Map[Long, Column] = mutable.Map()
  // 字段执行过程的关系
  private val fieldProcess: mutable.Map[Long, mutable.Set[Long]] = mutable.Map()
  // 压缩后的血缘关系 只记录source表到 target表
  private val fieldLineage: mutable.Map[Column, mutable.Set[Column]] = mutable.Map();
  // SQL类型 考虑、insert select、create as
  private var processType: String = ""



  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = withErrorHandling(qe) {
    // scuess exec logic plan exec
    lineageParser(qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = withErrorHandling(qe) {

  }

  private def withErrorHandling(qe: QueryExecution)(body: => Unit): Unit = {
    try
      body
    catch {
      case NonFatal(e) =>
        val ctx = qe.sparkSession.sparkContext
        logError(s"Unexpected error occurred during lineage processing for application: ${ctx.appName} #${ctx.applicationId}", e)
    }
  }

  //  qe: QueryExecution
  def lineageParser(qe: QueryExecution): Unit = {
    val columnToColumns: mutable.Map[Column, mutable.Set[Column]] = lineageParserPlan(qe.analyzed)
    println("|--------------------------------------------------------------|")
    println("|---------------column lineage parse result--------------------|")
    println("|--------------------------------------------------------------|")
    columnToColumns.keySet.foreach(target=>{
      val source: mutable.Set[Column] = columnToColumns.get(target).get
      println(s"| Target Column :\n|\t $target")
      println(s"| Source Column :")
      source.foreach(e=>println(s"|\t$e"))
      println("---------------------------------------------------------------")
    })
    println("<---------------------------------------------------------------->")

  }


//  qe: QueryExecution
  def lineageParserPlan(logicalPlan: LogicalPlan) = {
    logInfo("----------- field lineage parse start --------")
    resolveLogicV2(logicalPlan)
    // 关系连接
    connectSourceFieldAndTargetField()
    fieldLineage
  }

  /**
   *
   * @param plan
   */
  def resolveLogicV2(plan: LogicalPlan): Unit = {
    // 获取原始表从 LogicalRelation 或 HiveTableRelation 目标表从 InsertIntoHiveTable 和 CreateHiveTableAsSelectCommand
    // 获取转换过程从Aggregate 和 Project
    plan.collect {
      case plan: LogicalRelation => {
        val calalogTable = plan.catalogTable.get
        val tableName = calalogTable.database + "." + calalogTable.identifier.table
        plan.output.foreach(columnAttribute => {
          val columnFullName = tableName + "." + columnAttribute.name
          sourceTableColumns += (columnAttribute.exprId.id -> new Column(columnAttribute.exprId.id.toInt,LineageData.ColumnType.COLUMN,columnFullName,calalogTable.database,tableName,columnAttribute.name)
          )
        })
      }
      case plan: HiveTableRelation => {
        val tableName = plan.tableMeta.database + "." + plan.tableMeta.identifier.table
        plan.output.foreach(columnAttribute => {
          val columnFullName = tableName + "." + columnAttribute.name
          sourceTableColumns += (columnAttribute.exprId.id -> new Column(columnAttribute.exprId.id.toInt,LineageData.ColumnType.COLUMN,columnFullName,plan.tableMeta.database,tableName,columnAttribute.name))
        })
      }
      case plan: InsertIntoHiveTable => {
        val tableName = plan.table.database + "." + plan.table.identifier.table
        extTargetTable(plan.table.database,tableName, plan.query)
      }
      case plan: InsertIntoHadoopFsRelationCommand=>{
        val catalogTable: CatalogTable = plan.catalogTable.get
        val tableName=catalogTable.database+"."+catalogTable.identifier.table
        extTargetTable(catalogTable.database,tableName, plan.query)
      }
      case plan: CreateHiveTableAsSelectCommand => {
        val tableName = plan.tableDesc.database + "." + plan.tableDesc.identifier.table
        extTargetTable(plan.tableDesc.database,tableName, plan.query)
      }
      case plan: Aggregate => {
        plan.aggregateExpressions.foreach(aggItem => {
          extFieldProcess(aggItem)
        }
        )
      }
      case plan: Project => {
        plan.projectList.toList.foreach {
          pojoItem => {
            extFieldProcess(pojoItem)
          }
        }
      }
      //      case `plan` => logInfo("******child plan******:\n" + plan)
    }
  }

  def extFieldProcess(namedExpression: NamedExpression): Unit = {
    //alias 存在转换关系 不然就是原本的值
    namedExpression match {
      case e:Alias =>{
        val sourceFieldId = e.exprId.id
        val targetFieldIdSet: mutable.Set[Long] = fieldProcess.getOrElse(sourceFieldId, mutable.Set.empty)
        e.references.foreach(attribute => {
          targetFieldIdSet += attribute.exprId.id
        })
        fieldProcess += (sourceFieldId -> targetFieldIdSet)
      }
      case _ =>
    }

  }

  def extTargetTable(database:String,tableName: String, plan: LogicalPlan): Unit = {
    logInfo("start ext target table")
    plan.output.foreach(columnAttribute => {
      val columnFullName = tableName + "." + columnAttribute.name
      targetTableColumns += (TargetTableColumnId(tableName,columnAttribute.exprId.id)-> new Column(columnAttribute.exprId.id.toInt,LineageData.ColumnType.COLUMN,columnFullName,database,tableName,columnAttribute.name))
    })
  }

  /**
   * 从过程中提取血缘：目标表 字段循环是否存在于 source表中，不存在的话从过程中寻找直到遇见目标表
   */
  def connectSourceFieldAndTargetField(): Unit = {
    val fieldIds = targetTableColumns.keySet
    fieldIds.foreach(fieldId => {
      val resTargetFieldName = targetTableColumns(fieldId)
      val resSourceFieldSet: mutable.Set[Column] = mutable.Set.empty[Column]
      if (sourceTableColumns.contains(fieldId.id)) {
        resSourceFieldSet += sourceTableColumns(fieldId.id)
      } else {
        val targetIdsTmp = findSourceField(fieldId.id)
        resSourceFieldSet ++= targetIdsTmp
      }
      fieldLineage += (resTargetFieldName -> resSourceFieldSet)
    })
  }

  def findSourceField(fieldId: Long): mutable.Set[Column] = {
    val resSourceFieldSet: mutable.Set[Column] = mutable.Set.empty[Column]
    if (fieldProcess.contains(fieldId)) {
      val fieldIds: mutable.Set[Long] = fieldProcess.getOrElse(fieldId, mutable.Set.empty)
      fieldIds.foreach(fieldId => {
        if (sourceTableColumns.contains(fieldId)) {
          resSourceFieldSet += sourceTableColumns(fieldId)
        } else {
          val sourceFieldSet = findSourceField(fieldId)
          resSourceFieldSet ++= sourceFieldSet
        }
      })
    }
    resSourceFieldSet
  }
}
