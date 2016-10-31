/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.migration.sql;


import io.mindmaps.concept.ResourceType;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps JDBC supported data types to Mindmaps types
 */
public class SQLType {

    private final static Map<Integer, ResourceType.DataType> mappings = new HashMap<Integer, ResourceType.DataType>(){
        {
            put(Types.ARRAY, null);
            put(Types.BIGINT, ResourceType.DataType.LONG);
            put(Types.BINARY, null);
            put(Types.BIT, null);
            put(Types.BLOB, ResourceType.DataType.STRING);
            put(Types.BOOLEAN, ResourceType.DataType.BOOLEAN);
            put(Types.CHAR, ResourceType.DataType.STRING);
            put(Types.CLOB, ResourceType.DataType.STRING);
            put(Types.DATALINK, null);
            put(Types.DATE, ResourceType.DataType.STRING);
            put(Types.DECIMAL, ResourceType.DataType.DOUBLE);
            put(Types.DISTINCT, ResourceType.DataType.STRING);
            put(Types.DOUBLE, ResourceType.DataType.DOUBLE);
            put(Types.FLOAT, ResourceType.DataType.DOUBLE);
            put(Types.INTEGER, ResourceType.DataType.LONG);
            put(Types.JAVA_OBJECT, null);
            put(Types.LONGNVARCHAR, ResourceType.DataType.STRING);
            put(Types.LONGVARBINARY, ResourceType.DataType.STRING);
            put(Types.LONGVARCHAR, ResourceType.DataType.STRING);
            put(Types.NCHAR, ResourceType.DataType.STRING);
            put(Types.NCLOB, ResourceType.DataType.STRING);
            put(Types.NULL, null);
            put(Types.NUMERIC, ResourceType.DataType.DOUBLE);
            put(Types.NVARCHAR, ResourceType.DataType.STRING);
            put(Types.OTHER, ResourceType.DataType.STRING);
            put(Types.REAL, ResourceType.DataType.STRING);
            put(Types.REF, ResourceType.DataType.STRING);
            put(Types.REF_CURSOR, ResourceType.DataType.STRING);
            put(Types.ROWID, ResourceType.DataType.LONG);
            put(Types.SMALLINT, ResourceType.DataType.LONG);
            put(Types.SQLXML, null);
            put(Types.STRUCT, null);
            put(Types.TIME, ResourceType.DataType.STRING);
            put(Types.TIME_WITH_TIMEZONE, ResourceType.DataType.STRING);
            put(Types.TIMESTAMP, ResourceType.DataType.STRING);
            put(Types.TIMESTAMP_WITH_TIMEZONE, ResourceType.DataType.STRING);
            put(Types.TINYINT, ResourceType.DataType.LONG);
            put(Types.VARBINARY, ResourceType.DataType.STRING);
            put(Types.VARCHAR, ResourceType.DataType.STRING);
        }};

    /**
     * Get the SQLType given a SQL String. The default is varchar (String)
     */
    static ResourceType.DataType getDatatype(int sqlType) {
        ResourceType.DataType datatype = mappings.get(sqlType);
        if(datatype == null){
            return mappings.get(Types.VARCHAR);
        }
        return datatype;
    }
}
