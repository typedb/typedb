/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.cache;

import com.google.common.collect.ImmutableSet;
import graql.lang.statement.Variable;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 *
 * Index for IndexedAnswerSet. Corresponds to a set of variables which partial substitutions need to map.
 *
 */
public class Index{
    final private ImmutableSet<Variable> vars;

    private Index(Variable var){
        this.vars = ImmutableSet.of(var);
    }

    private Index(Set<Variable> vars){
        this.vars = ImmutableSet.copyOf(vars);
    }

    public static Index empty(){
        return new Index(new HashSet<>());
    }

    public static Index of(Variable var){
        return new Index(var);
    }

    public static Index of (Set<Variable> vars){
        return new Index(vars);
    }

    Set<Variable> vars(){return vars;}

    @Override
    public String toString(){ return vars.toString(); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Index index = (Index) o;
        return Objects.equals(vars, index.vars);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vars);
    }
}
