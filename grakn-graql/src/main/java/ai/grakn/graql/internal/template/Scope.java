/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.template;

import ai.grakn.graql.Var;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    private final Set<Var> variablesEncountered;

    public Scope(Map<String, Object> data){
        this.parent = null;
        this.values = data;
        this.variablesEncountered = new HashSet<>();
    }

    public Scope(Scope parent, Map<String, Object> data){
        this.parent = parent;
        this.values = data;
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
     * Retrieve the value of a key from this scope
     * @param var key to retrieve
     * @return value associated with the provided key
     */
    public Object resolve(String var) {
        return values.get(var);
    }

    /**
     * Retrieve the data in the current scope
     * @return data in this scope
     */
    public Map data(){
        return this.values;
    }

    /**
     * Check if the variable has been seen before in this scope
     * @param variable variable to check the presence of
     * @return true if the variable has been seen, false otherwise
     */
    boolean hasSeen(Var variable){
        return variablesEncountered.contains(variable);
    }

    /**
     * Mark a variable as seen within this scope
     * @param variable variable to mark as seen
     */
    void markAsSeen(Var variable){
        variablesEncountered.add(variable);
    }
}