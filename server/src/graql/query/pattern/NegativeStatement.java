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

package grakn.core.graql.query.pattern;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;
import grakn.core.graql.internal.executor.property.PropertyExecutor;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO
 */
public class NegativeStatement extends Statement {

    public NegativeStatement(Variable var, Set<VarProperty> properties) {
        super(var, properties);
    }

    @Override
    public boolean isPositive() { return false; }

    @Override
    public Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm() {
        //flatten if multiple properties present

        //TODO this is hacky. Redo when atoms are independent of transactions.
        HashMultimap<VarProperty, VarProperty> propertyMap = HashMultimap.create();
        properties()
                .forEach(p -> {
                    if (PropertyExecutor.atomable(this.var(), p).mappable(this))
                        propertyMap.put(p, p);
                    else
                        getProperties(RelationProperty.class).forEach(rp -> propertyMap.put(rp, p));
                });

        Set<Conjunction<Statement>> patterns = propertyMap.asMap().entrySet().stream()
                .map(e -> new NegativeStatement(var(), Sets.newHashSet(e.getValue())))
                .map(Statement::asStatement)
                .map(p -> Pattern.and(Collections.singleton(p)))
                .collect(Collectors.toSet());

        return Pattern.or(patterns);
    }

    @Override
    public Pattern negate(){
        return new PositiveStatement(var(), properties());
    }

    @Override
    public String toString(){ return "NOT " + super.toString();}
}

