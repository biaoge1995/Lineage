package com.ddmc;

import com.ddmc.api.LineageData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName com.ddmc.LineageData
 * @Description TODO
 * @Author chenbiao
 * @Date 2023/4/21 4:23 下午
 * @Version 1.0
 **/
public class HiveLineageData extends LineageData{


    public static class HiveColumn extends LineageData.Column {

        private String[] vertexIdSplit;
        private boolean hasTable=true;


        public String getDataBase() {
            if(vertexIdSplit.length==1){
                return "";
            }else {
                return vertexIdSplit[0];
            }
        }

        public String getTable() {
            if (vertexIdSplit.length >= 2) {
                return vertexIdSplit[0] + "." + vertexIdSplit[1];
            }
            return "";
        }

        public String getColumnName() {
            if (getColumnType()== ColumnType.COLUMN) {
                if (vertexIdSplit.length == 3) {
                    return vertexIdSplit[2];
                } else if (vertexIdSplit.length == 1)
                    return vertexIdSplit[0];
            }
            return getColumnId();
        }

        @Override
        public void setColumnId(String columnId) {
            vertexIdSplit = columnId.split("\\.");
            super.setColumnId(columnId);
        }

        public boolean isHasTable() {
            if(vertexIdSplit.length==1){
                hasTable=false;
            }
            return hasTable;
        }
    }



}

