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

package io.mindmaps.migration.template;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Scope {

    private Scope parent;
    private Map<String, Value> values;
    private Set<String> variables;

    public Scope(){
        this(null, Collections.emptySet(), Collections.emptyMap());
    }

    public Scope(Scope parent,
                 Set<String> variables,
                 Map<String, Object> data){

        this.parent = parent;
        this.variables = localVariables(this.parent, variables);
        this.values = new HashMap<>();
        this.assign("", data);
    }

    public Scope up() {
        return parent;
    }

    @SuppressWarnings("unchecked")
    public void assign(Object value) {
        if (value instanceof Map) {
            assign("", value);
        } else {
            assign(".", value);
        }
    }

    public Value resolve(String var) {
        Value value = values.get(var);

        if(value != null) {
            // The variable resides in this scope
            return value;
        }
        else if(!isGlobalScope()) {
            // Let the parent scope look for the variable
            return parent.resolve(var);
        }
        else {
            // Unknown variable
            return Value.NULL;
        }
    }

    public boolean isGlobalScope() {
        return parent == null;
    }

    public Set<String> variables(){
        return variables;
    }

    private Set<String> localVariables(Scope scope, Set<String> currentVariables){
        if(scope == null){
           return currentVariables;
        }

        currentVariables.removeAll(scope.variables());
        return localVariables(scope.parent, currentVariables);
    }

    @SuppressWarnings("unchecked")
    private void assign(String prefix, Object value){
        if(value instanceof Map){
            Map<String, Object> map = (Map) value;

            if(!prefix.isEmpty()){
                prefix = prefix + ".";
            }

            for(String key: map.keySet()) {
                assign(prefix  + key, map.get(key));
            }
        }
        else {
            values.put(prefix, new Value(value));
        }
    }
}