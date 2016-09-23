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
import java.util.Map;
import java.util.Set;

import static io.mindmaps.migration.template.ValueFormatter.format;
import static java.util.stream.Collectors.toMap;

public class Scope {

    private Scope parent;
    private Map<Variable, Value> variables;

    public Scope(){
        this(null, Collections.emptySet(), Collections.emptyMap());
    }

    public Scope(Scope parent,
                 Set<Variable> variables,
                 Map<String, Object> data){

        this.parent = parent;
        this.variables = localVariables(this.parent, variables)
                .stream()
                .collect(toMap(
                        v -> v,
                        v -> Value.VOID));
        this.putData(data);
    }

    public Scope up() {
        return parent;
    }

    @SuppressWarnings("unchecked")
    public void assign(Variable variable, Object value) {
        if (value instanceof Map) {
            this.putData((Map) value);
        } else {
            this.variables.put(variable, new Value(value));
        }
    }

    public Value resolve(Variable var) {
        Value value = variables.get(var);
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
            return null;
        }
    }

    public boolean isLocalVar(Variable var){
        return variables.keySet().contains(var);
    }

    public boolean isGlobalScope() {
        return parent == null;
    }

    public Set<Variable> variables(){
        return variables.keySet();
    }

    private Set<Variable> localVariables(Scope scope, Set<Variable> currentVariables){
        if(scope == null){
           return currentVariables;
        }

        currentVariables.removeAll(scope.variables.keySet());
        return localVariables(scope.parent, currentVariables);
    }

    private void putData(Map<String, Object> data){
        data.entrySet()
                .forEach(e -> variables.put(new Variable(e.getKey()), new Value(e.getValue())));
    }
}