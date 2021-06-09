/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.common;

import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typeql.lang.TypeQL;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class TypeQLHelpers {
    /**
     * Create a set of variables that will query for the keys of the concepts given in the map. Attributes given in
     * the map are simply queried for by their own type and value.
     * @param varMap variable map of concepts
     * @return Variables that check for the keys of the given concepts
     */
    public static Set<Variable> generateKeyVariables(Map<Variable, Concept> varMap) {
        LinkedHashSet<Variable> variables = new LinkedHashSet<>();

        for (Map.Entry<Variable, Concept> entry : varMap.entrySet()) {
            Variable var = entry.getKey();
            Concept concept = entry.getValue();

            if (concept.isAttribute()) {

                String typeLabel = concept.asAttribute().type().label().toString();
                Variable variable = TypeQL.var(var).isa(typeLabel);
                VariableAttribute s = null;

                Object attrValue = concept.asAttribute().value();
                if (attrValue instanceof String) {
                    s = variable.val((String) attrValue);
                } else if (attrValue instanceof Double) {
                    s = variable.val((Double) attrValue);
                } else if (attrValue instanceof Long) {
                    s = variable.val((Long) attrValue);
                } else if (attrValue instanceof LocalDateTime) {
                    s = variable.val((LocalDateTime) attrValue);
                } else if (attrValue instanceof Boolean) {
                    s = variable.val((Boolean) attrValue);
                }
                variables.add(s);

            } else if (concept.isEntity() | concept.isRelation()){

                concept.asThing().keys().forEach(attribute -> {

                    String typeLabel = attribute.type().label().toString();
                    Variable variable = TypeQL.var(var);
                    Object attrValue = attribute.value();

                    VariableInstance s = null;
                    if (attrValue instanceof String) {
                        s = variable.has(typeLabel, (String) attrValue);
                    } else if (attrValue instanceof Double) {
                        s = variable.has(typeLabel, (Double) attrValue);
                    } else if (attrValue instanceof Long) {
                        s = variable.has(typeLabel, (Long) attrValue);
                    } else if (attrValue instanceof LocalDateTime) {
                        s = variable.has(typeLabel, (LocalDateTime) attrValue);
                    } else if (attrValue instanceof Boolean) {
                        s = variable.has(typeLabel, (Boolean) attrValue);
                    }
                    variables.add(s);
                });

            } else {
                throw new ResolutionConstraintException("Presently we only handle queries concerning Things, not Types");
            }
        }
        return variables;
    }

    public static Variable makeAnonVarsExplicit(Variable variable) {
        if (variable.var().isReturned()) {
            return variable;
        } else {
            return Variable.create(variable.var().asReturnedVar(), variable.properties());
        }
    }
}
