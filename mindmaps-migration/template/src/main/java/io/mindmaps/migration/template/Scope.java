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

import mjson.Json;

import java.util.HashMap;
import java.util.Map;

import static io.mindmaps.migration.template.ValueFormatter.format;

public class Scope {

    private Scope parent;
    private Map<String, Value> variables;
    private String block;

    public Scope(){
        this(null);
    }

    public Scope(Scope parent){
        this.parent = parent;
        this.variables = new HashMap<>();
    }

    public Scope up() {
        return parent;
    }

    public void assign(String variable, Object value) {
        if (value instanceof Map) {
            assign((Map) value);
        } else {
            this.variables.put(variable, new Value(value));
        }
    }

    public void assign(Map<String, Object> context) {
        context.entrySet().stream()
                .forEach(e -> variables.put(e.getKey(), new Value(e.getValue())));
    }

    public boolean isGlobalScope() {
        return parent == null;
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
}