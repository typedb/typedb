/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.template;

import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class Scope {

    private Scope parent;
    private Map<String, Value> values;
    private Set<String> variablesEncountered;

    public Scope(Map<String, Object> data){
        this.values = new HashMap<>();
        this.variablesEncountered = new HashSet<>();
        assign("", data);
    }

    public Scope(Scope parent){
        this.parent = parent;
        this.values = new HashMap<>();
        this.variablesEncountered = Sets.newHashSet(parent.variablesEncountered);
    }

    public Scope up() {
        return parent;
    }

    @SuppressWarnings("unchecked")
    public void assign(String prefix, Object value){
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
            values.put(prefix, value == null ? Value.NULL : new Value(value));
        }
    }

    public void unassign(String prefix){
        Set<String> removed = values.keySet().stream()
                .filter(s -> s.startsWith(prefix))
                .collect(toSet());

        removed.forEach(values::remove);
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

    public boolean hasSeen(String variable){
        return variablesEncountered.contains(variable);
    }

    public void markAsSeen(String variable){
        variablesEncountered.add(variable);
    }
}