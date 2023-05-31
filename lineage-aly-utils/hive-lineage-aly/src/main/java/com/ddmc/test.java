package com.ddmc;

import com.ddmc.HiveLineage;
import com.ddmc.api.LineageData;
import com.ddmc.graph.Load2neo4j;

/**
 * @ClassName com.ddmc.TestDriver
 * @Description TODO
 * @Author chenbiao
 * @Date 2023/4/19 3:33 下午
 * @Version 1.0
 **/
public class test {


    private static final String NEO4J_BOLT_URL = "bolt://localhost:7687";
    private static final String NEO4J_USERNAME = "neo4j";
    private static final String NEO4J_PASSWORD = "365530302";

    public static void main(String[] args) throws Exception {


        String sql = "create table dwd.student4 as\n" +
                "select student_name2,count(student_name) as cnt\n" +
                "      from\n" +
                "      (select concat(student_name,'-',b.product_Id) as student_name2,a.*\n" +
                "      from\n" +
                "      (select\n" +
                "      if(id='1',id,concat(student_id,'-',name)) as student_name\n" +
                "      ,c.store_name\n" +
                "      ,b.score\n" +
                "      ,id,name\n" +
                "      from student a\n" +
                "      left join score b\n" +
                "       ON a.id = b.student_id\n" +
                "      inner join dim.store c\n" +
                "       ON a.id = c.store_id\n" +
                "       and c.snapshot = '2023-04-25'\n" +
                "      where name='chenbiao'\n" +
                "      ) a\n" +
                "      left join dim.product_hive b\n" +
                "       ON a.id = b.product_id\n" +
                "      ) t\n" +
                "      group by student_name2 ";

        HiveLineage hiveLineage = new HiveLineage();
        Load2neo4j load2neo4j = new Load2neo4j(NEO4J_BOLT_URL, NEO4J_USERNAME, NEO4J_PASSWORD);
        LineageData lineAgeData = hiveLineage.getLineAgeData(sql);
        load2neo4j.loadDataFromSql(lineAgeData);


    }
}
