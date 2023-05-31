package com.ddmc.api;

import java.util.*;

/**
 * @ClassName com.ddmc.LineageData
 * @Description TODO
 * @Author chenbiao
 * @Date 2023/4/21 4:23 下午
 * @Version 1.0
 **/
public class LineageData {
    private String version;
    private String user;
    private long timestamp;
    private long duration;
    private String engine;
    private String database;
    private String hash;
    private String queryText;
    //血缘关系图的边
    private List<EdgeInfo> edges = new ArrayList<>();
    //血缘关系图的顶点
    private Set<Column> columns = new HashSet<>();
    private HashMap<Integer,Column> columnMap = new HashMap<Integer,Column>();



    public Column getColumnById(int id) {
        return columnMap.get(id);
    }

    public void addColumn(Column column){
        columnMap.put(column.id,column);
        columns.add(column);

    }

    @Override
    public String toString() {
        return "LineageData{" +
                "version='" + version + '\'' +
                ", user='" + user + '\'' +
                ", timestamp=" + timestamp +
                ", duration=" + duration +
                ", engine='" + engine + '\'' +
                ", database='" + database + '\'' +
                ", hash='" + hash + '\'' +
                ", queryText='" + queryText + '\'' +
                ", edges=" + edges +
                ", columns=" + columns +
                '}';
    }

    public static  enum EdgeType {
        PROJECT
        , PREDICATE //谓词

    }

    public static class EdgeInfo {
        //源头
        private Set<Column> sources = new HashSet<>();
        private Set<Integer> sourceIds = new HashSet<>();
        //目标
        private Set<Column> targets = new HashSet<>();
        private Set<Integer> targetIds = new HashSet<>();
        private String expression;
        private EdgeType edgeType;

        @Override
        public String toString() {
            return "EdgeInfo{" +
                    "sources=" + sources +
                    ", targets=" + targets +
                    ", expression='" + expression + '\'' +
                    ", edgeType=" + edgeType +
                    '}';
        }

        public void addSourceId(int sourceId){
            sourceIds.add(sourceId);
        }

        public void addTargetId(int targetId){
            targetIds.add(targetId);
        }


        public void addSource(Column column){
            sources.add(column);
        }

        public void addTarget(Column column){
            targets.add(column);
        }

        public Set<Column> getSources() {
            return sources;
        }
        public void setSources(Set<Column> sources) {
            this.sources = sources;
        }
        public Set<Column> getTargets() {
            return targets;
        }
        public void setTargets(Set<Column> targets) {
            this.targets = targets;
        }

        public String getExpression() {
            return expression;
        }
        public void setExpression(String expression) {
            this.expression = expression;
        }

        public EdgeType getEdgeType() {
            return edgeType;
        }

        public void setEdgeType(EdgeType edgeType) {
            this.edgeType = edgeType;
        }
    }

    public static  enum ColumnType {
        COLUMN, TABLE
    }

    public  static class Column {

        private int id;
        private ColumnType columnType;
        //顶点的唯一id
        private String columnId;

        private String dataBase;

        //table的全名 xx.table
        private String table;

        private String columnName;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Column column = (Column) o;
            return columnId.equals(column.columnId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnId);
        }

        @Override
        public String toString() {
            return table+"."+columnName;
        }

        public Column(int id, ColumnType columnType, String columnId, String dataBase, String table,String columnName) {
            this.id = id;
            this.columnType = columnType;
            this.columnId = columnId;
            this.dataBase = dataBase;
            this.table = table;
            this.columnName = columnName;
        }

        public Column() {
        }

        public void setDataBase(String dataBase) {
            this.dataBase = dataBase;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getDataBase() {
            return dataBase;
        }

        public String getTable() {
            return table;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public int getId() {
            return id;
        }
        public void setId(int id) {
            this.id = id;
        }

        public ColumnType getColumnType() {
            return columnType;
        }

        public void setColumnType(ColumnType columnType) {
            this.columnType = columnType;
        }

        public String getColumnId() {
            return columnId;
        }
        public void setColumnId(String columnId) {
            this.columnId = columnId;
        }

    }

    public String getVersion() {
        return version;
    }
    public void setVersion(String version) {
        this.version = version;
    }
    public String getUser() {
        return user;
    }
    public void setUser(String user) {
        this.user = user;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public long getDuration() {
        return duration;
    }
    public void setDuration(long duration) {
        this.duration = duration;
    }
    public String getEngine() {
        return engine;
    }
    public void setEngine(String engine) {
        this.engine = engine;
    }
    public String getDatabase() {
        return database;
    }
    public void setDatabase(String database) {
        this.database = database;
    }
    public String getHash() {
        return hash;
    }
    public void setHash(String hash) {
        this.hash = hash;
    }
    public String getQueryText() {
        return queryText;
    }
    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }
    public List<EdgeInfo> getEdges() {
        return edges;
    }
    public void setEdges(List<EdgeInfo> edges) {
        this.edges = edges;
    }
    public Set<Column> getColumns() {
        return columns;
    }



    public void setColumns(Set<Column> columns) {
        this.columns = columns;
    }



}

