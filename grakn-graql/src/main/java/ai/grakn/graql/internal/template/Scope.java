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
import org.apache.commons.lang.ObjectUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Represents a scope (association of name to a value) and corresponds to a block
 * in the TemplateVisitor class.
 *
 * This can also be thought of as a block in Graql Temaplates. I.e. any body of text
 * surrounded by {} or the root.
 *
 * @author alexandraorth
 */
public class Scope {

    private final Scope parent;
    private final Map<String, Object> values;
    private final Set<String> variablesEncountered;

    public Scope(Map<String, Object> data){
        parent = null;
        this.values = new HashMap<>();
        this.variablesEncountered = new HashSet<>();
        assign("", data);
    }

    public Scope(Scope parent){
        this.parent = parent;
        this.values = new HashMap<>();
        this.variablesEncountered = Sets.newHashSet(parent.variablesEncountered);
    }

    /**
     * Move up one level to the parent scope
     * @return the parent scope
     */
    public Scope up() {
        return parent;
    }

    /**
     * Associate a value with the given key. If the given value is a map, recurse through it and concatenate
     * the given prefix with the keys in the map separated by ".".
     *
     * @param prefix key to use in the map of values.
     * @param value value to associate.
     */
    @SuppressWarnings("unchecked")
    public void assign(String prefix, Object value){
        if(value instanceof Map){
            Map<String, Object> map = (Map) value;

            if(!prefix.isEmpty()){
                prefix = prefix + ".";
            }

            for(Map.Entry<String, Object> entry: map.entrySet()) {
                assign(prefix + entry.getKey(), entry.getValue());
            }
        }
        else {
            values.put(prefix, value == null ? null : value);
        }
    }

    /**
     * Remove a key/value pair from this scope
     * @param prefix key to remove from the scope
     */
    public void unassign(String prefix){
        Set<String> removed = values.keySet().stream()
                .filter(s -> s.startsWith(prefix))
                .collect(toSet());

        removed.forEach(values::remove);
    }

    /**
     * Retrieve the value of a key from this scope, or the parent scope if it is not present in the current one.
     * @param var key to retrieve
     * @return value associated with the provided key
     */
    public Object resolve(String var) {
        Object value = values.get(var);

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
            return ObjectUtils.NULL;
        }
    }

    /**
     * Check if this scope is "global", i.e. has no parent
     * @return true if this is the "root" scope
     */
    public boolean isGlobalScope() {
        return parent == null;
    }

    /**
     * Check if the variable has been seen before in this scope
     * @param variable variable to check the presence of
     * @return true if the variable has been seen, false otherwise
     */
    public boolean hasSeen(String variable){
        return variablesEncountered.contains(variable);
    }

    /**
     * Mark a variable as seen within this scope
     * @param variable variable to mark as seen
     */
    public void markAsSeen(String variable){
        variablesEncountered.add(variable);
    }
}