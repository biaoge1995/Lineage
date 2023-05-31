package com.ddmc.graph;

import com.ddmc.api.LineAgeAly;
import com.ddmc.api.LineageData;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @ClassName com.ddmc.TestDriver
 * @Description TODO
 * @Author chenbiao
 * @Date 2023/4/19 3:33 下午
 * @Version 1.0
 **/
public class Load2neo4j {



    private static Session neo4jSession = null;

    private static org.neo4j.driver.Driver neo4jDriver = null;
    private String url;
    private String userName;
    private String passwd;



    public Load2neo4j(String url, String userName, String passwd) {
        this.url = url;
        this.userName = userName;
        this.passwd = passwd;
    }

    public void loadDataFromSql(String sql, LineAgeAly lineAgeAly) throws Exception {
        inertNeo4jTest(lineAgeAly.getLineAgeData(sql));
    }

    public void loadDataFromSql(LineageData lineageData) throws Exception {
        inertNeo4jTest(lineageData);
    }




    /**
     * 单例获取neo4j session
     *
     * @return
     */
    synchronized private  Session getNeo4jSession() {
        if (neo4jSession == null || !neo4jSession.isOpen()) {
            if (neo4jDriver != null) {
            } else {
                neo4jDriver = GraphDatabase.driver(url, AuthTokens.basic(userName, passwd));
            }
            neo4jSession = neo4jDriver.session();
            return neo4jSession;
        } else {
            return neo4jSession;
        }

    }




    /**
     * 测试手动插入数据以及维护关系
     */
    private void inertNeo4jTest(LineageData lineageData) {
        // 构造数据， 数据和pg 库里面的数据一样
        Set<LineageData.Column> vertices = lineageData.getColumns();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        // 手动create to neo4j
        Set<String> deleteTables = new HashSet<>();
        String deleteTemplate = "match(c:column {table: '$table'}) detach delete c";
        for (LineageData.EdgeInfo edge : lineageData.getEdges()) {
            for (LineageData.Column Column : edge.getTargets()) {
                if (!StringUtils.isEmpty(Column.getTable())) {
                    deleteTables.add(Column.getTable());
                }

            }
        }
        for (String deleteTable : deleteTables) {
            String CQL = deleteTemplate.replace("$table", deleteTable);
            getNeo4jSession().run(CQL);
        }
        System.out.println("删除表节点成功耗时： " + stopWatch.getTime() + " ms");


        String[] createCOLTemplates = {"merge (col:column {id: $id, vertexType: '$vertexType', vertexId: '$vertexId',name: '$name',table:'$table',database: '$database'})"};
//                , "merge (tab:table {vertexType: 'TABLE', vertexId: '$table',name:'$table',database: '$database'})"
//                , "match (col:column {vertexId: '$vertexId'}),(tab:table {vertexId: '$table'}) merge (col)-[r:belong]->(tab)"

        for (LineageData.Column data : vertices) {
            //节点类型为表的忽略，无价值
            if (data.getColumnType().equals("TABLE")) {
                continue;
            }
            for (String CQL : createCOLTemplates) {
                String createCQL = CQL.replace("$id", String.valueOf(data.getId()))
                        .replace("$vertexType", data.getColumnType().toString())
                        .replace("$vertexId", data.getColumnId())
                        .replace("$name", data.getColumnName())
                        .replace("$table", data.getTable())
                        .replace("$database", data.getDataBase())
                        .replace("$hash", lineageData.getHash());
                getNeo4jSession().run(createCQL);
            }


        }
        ;
        System.out.println("插入成功耗时： " + stopWatch.getTime() + " ms");
        // 手动维护关系
        String mergeCQLTemplate = "match (a:column{id: $id,vertexId:'$AvertexId' }), (b:column{id: $bid,vertexId:'$BvertexId'}) MERGE(a)-[:$edgeType {expression: \"$expression\"}]->(b)";

        for (LineageData.EdgeInfo edge : lineageData.getEdges()) {
            if (edge.getEdgeType() == LineageData.EdgeType.PREDICATE) {
                continue;
            }
            for (LineageData.Column sourceColumn : edge.getSources()) {
                for (LineageData.Column targetColumn :edge.getTargets()) {
                    String mergeCQL = mergeCQLTemplate
                            .replace("$id", String.valueOf(sourceColumn.getId()))
                            .replace("$bid", String.valueOf(targetColumn.getId()))
                            .replace("$AvertexId", sourceColumn.getColumnId())
                            .replace("$BvertexId", targetColumn.getColumnId())
                            .replace("$edgeType", edge.getEdgeType().toString())
                            .replace("$expression", edge.getExpression()==null?"":edge.getExpression());

                    getNeo4jSession().run(mergeCQL);


                }
            }
        }

        stopWatch.stop();
        System.out.println("转换关系成功，耗时： " + stopWatch.getTime() + " ms");

        // close resource
        getNeo4jSession().close();
        neo4jDriver.close();
        System.out.println(neo4jSession);
        System.out.println(neo4jDriver);
    }


}
