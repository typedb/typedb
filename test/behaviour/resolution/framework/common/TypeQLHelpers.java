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
     * Create a set of statements that will query for the keys of the concepts given in the map. Attributes given in
     * the map are simply queried for by their own type and value.
     * @param varMap variable map of concepts
     * @return Statements that check for the keys of the given concepts
     */
    public static Set<Statement> generateKeyStatements(Map<Variable, Concept> varMap) {
        LinkedHashSet<Statement> statements = new LinkedHashSet<>();

        for (Map.Entry<Variable, Concept> entry : varMap.entrySet()) {
            Variable var = entry.getKey();
            Concept concept = entry.getValue();

            if (concept.isAttribute()) {

                String typeLabel = concept.asAttribute().type().label().toString();
                Statement statement = TypeQL.var(var).isa(typeLabel);
                StatementAttribute s = null;

                Object attrValue = concept.asAttribute().value();
                if (attrValue instanceof String) {
                    s = statement.val((String) attrValue);
                } else if (attrValue instanceof Double) {
                    s = statement.val((Double) attrValue);
                } else if (attrValue instanceof Long) {
                    s = statement.val((Long) attrValue);
                } else if (attrValue instanceof LocalDateTime) {
                    s = statement.val((LocalDateTime) attrValue);
                } else if (attrValue instanceof Boolean) {
                    s = statement.val((Boolean) attrValue);
                }
                statements.add(s);

            } else if (concept.isEntity() | concept.isRelation()){

                concept.asThing().keys().forEach(attribute -> {

                    String typeLabel = attribute.type().label().toString();
                    Statement statement = TypeQL.var(var);
                    Object attrValue = attribute.value();

                    StatementInstance s = null;
                    if (attrValue instanceof String) {
                        s = statement.has(typeLabel, (String) attrValue);
                    } else if (attrValue instanceof Double) {
                        s = statement.has(typeLabel, (Double) attrValue);
                    } else if (attrValue instanceof Long) {
                        s = statement.has(typeLabel, (Long) attrValue);
                    } else if (attrValue instanceof LocalDateTime) {
                        s = statement.has(typeLabel, (LocalDateTime) attrValue);
                    } else if (attrValue instanceof Boolean) {
                        s = statement.has(typeLabel, (Boolean) attrValue);
                    }
                    statements.add(s);
                });

            } else {
                throw new ResolutionConstraintException("Presently we only handle queries concerning Things, not Types");
            }
        }
        return statements;
    }

    public static Statement makeAnonVarsExplicit(Statement statement) {
        if (statement.var().isReturned()) {
            return statement;
        } else {
            return Statement.create(statement.var().asReturnedVar(), statement.properties());
        }
    }
}
