package io.mindmaps.migration.sql;

import io.mindmaps.core.Data;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps JDBC supported data types to Mindmaps types
 */
public class SQLType {

    private final static Map<Integer, Data> mappings = new HashMap<Integer, Data>(){
        {
            put(Types.ARRAY, null);
            put(Types.BIGINT, Data.LONG);
            put(Types.BINARY, null);
            put(Types.BIT, null);
            put(Types.BLOB, Data.STRING);
            put(Types.BOOLEAN, Data.BOOLEAN);
            put(Types.CHAR, Data.STRING);
            put(Types.CLOB, Data.STRING);
            put(Types.DATALINK, null);
            put(Types.DATE, Data.STRING);
            put(Types.DECIMAL, Data.DOUBLE);
            put(Types.DISTINCT, Data.STRING);
            put(Types.DOUBLE, Data.DOUBLE);
            put(Types.FLOAT, Data.DOUBLE);
            put(Types.INTEGER, Data.LONG);
            put(Types.JAVA_OBJECT, null);
            put(Types.LONGNVARCHAR, Data.STRING);
            put(Types.LONGVARBINARY, Data.STRING);
            put(Types.LONGVARCHAR, Data.STRING);
            put(Types.NCHAR, Data.STRING);
            put(Types.NCLOB, Data.STRING);
            put(Types.NULL, null);
            put(Types.NUMERIC, Data.DOUBLE);
            put(Types.NVARCHAR, Data.STRING);
            put(Types.OTHER, Data.STRING);
            put(Types.REAL, Data.STRING);
            put(Types.REF, Data.STRING);
            put(Types.REF_CURSOR, Data.STRING);
            put(Types.ROWID, Data.LONG);
            put(Types.SMALLINT, Data.LONG);
            put(Types.SQLXML, null);
            put(Types.STRUCT, null);
            put(Types.TIME, Data.STRING);
            put(Types.TIME_WITH_TIMEZONE, Data.STRING);
            put(Types.TIMESTAMP, Data.STRING);
            put(Types.TIMESTAMP_WITH_TIMEZONE, Data.STRING);
            put(Types.TINYINT, Data.LONG);
            put(Types.VARBINARY, Data.STRING);
            put(Types.VARCHAR, Data.STRING);
        }};

    /**
     * Get the SQLType given a SQL String. The default is varchar (String)
     */
    static Data getDatatype(int sqlType) {
        Data datatype = mappings.get(sqlType);
        if(datatype == null){
            return mappings.get(Types.VARCHAR);
        }
        return datatype;
    }
}
