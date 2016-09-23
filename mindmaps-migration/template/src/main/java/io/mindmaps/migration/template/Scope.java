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

import java.util.*;

import static io.mindmaps.migration.template.ValueFormatter.format;
import static java.util.stream.Collectors.toMap;

public class Scope {

    private Scope parent;
    private Map<String, Value> variables;
    private Set<String> localGraqlVariables;

    public Scope(){
        this(null, Collections.emptyMap(), Collections.emptySet());
    }

    public Scope(Scope parent,
                 Map<String, Object> data,
                 Set<String> graqlVariables){
        this.parent = parent;

        this.variables = convertMap(data);
        this.localGraqlVariables = getLocalVariables(this.parent, graqlVariables);
    }

    public Scope up() {
        return parent;
    }

    public void assign(String variable, Object value) {
        if (value instanceof Map) {
            this.variables.putAll(convertMap((Map) value));
        } else {
            this.variables.put(variable, new Value(value));
        }
    }

    public Value resolve(String var) {
        var = var.replace("%", "");

        Value value = variables.get(var);
        if(value != null) {
            // The variable resides in this scope
            return format(value);
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

    public boolean isLocalVar(String var){
        return localGraqlVariables.contains(var);
    }

    public boolean isGlobalScope() {
        return parent == null;
    }

    private Set<String> getLocalVariables(Scope scope, Set<String> currentVariables){
        if(scope == null){
           return currentVariables;
        }

        currentVariables.removeAll(scope.localGraqlVariables);
        return getLocalVariables(scope.parent, currentVariables);
    }

    private Map<String, Value> convertMap(Map<String, Object> toConvert){
        return toConvert.entrySet().stream()
            .collect(toMap(
                    Map.Entry::getKey,
                    e -> new Value(e.getValue())
            ));
    }
}