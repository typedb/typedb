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

package io.mindmaps.graql.internal.template;

import java.util.List;

public class Value {

    public static final Value NULL = new Value();
    public static final Value VOID = new Value();

    private Object value;

    private Value(){
        value = new Object();
    }

    public Value(Object value){
        if(value == null){
            throw new RuntimeException("value is null");
        }

        this.value = value;

        if(!(isString() || isList()|| isBoolean() || isNumber())){
            throw new RuntimeException("unsupported type for " + value);
        }
    }

    public boolean isString(){
        return value instanceof String;
    }

    public boolean isDouble(){
        return value instanceof Double;
    }

    public boolean isLong(){
        return value instanceof Long;
    }

    public boolean isNumber(){
        return value instanceof Number;
    }

    public boolean isList(){
        return value instanceof List;
    }

    public boolean isBoolean(){
        return value instanceof Boolean;
    }

    public boolean isNull(){
        return this == NULL;
    }

    public boolean isVoid(){
        return this == VOID;
    }

    public String asString(){
        return (String) value;
    }

    public double asDouble(){
        return (Double) value;
    }

    public long asLong(){
        return (Long) value;
    }

    public List<Object> asList(){
        return (List) value;
    }

    public boolean asBoolean(){
        return (Boolean) value;
    }

    public static Value concat(Object... objects){
        StringBuilder builder = new StringBuilder();
        for(Object value:objects){
            builder.append(value.toString());
        }

        return new Value(builder.toString());
    }

    public static Value format(Value val) {

        if(val.isString()){
            return new Value("\\\"" + val + "\\\"");
        }

        return val;
    }

    public static String formatVar(Value var){
        String variable = var.toString();
        if(variable.contains(" ")){
            return variable.replaceAll("(\\S)\\s(\\S)", "$1-$2");
        }

        return variable;
    }

    @Override
    public String toString() {
        return isNull() ? "null" : isVoid() ? "" : value.toString();
    }
}
