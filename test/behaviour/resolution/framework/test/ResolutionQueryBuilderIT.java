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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.test;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.soundness.ResolutionTestingException;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadComplexRecursionExample;
import static org.junit.Assert.assertEquals;


public class ResolutionQueryBuilderIT {

    private static final String database = "complex-recursion";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);

    @Test
    public void testKeysVariablesAreGeneratedCorrectly() throws IOException {
        Util.resetDirectory(dataDir);
        TypeQLMatch inferenceQuery = TypeQL.parseQuery("match $transaction isa transaction, has currency $currency;").asMatch();

        try (TypeDB typedb = RocksTypeDB.open(options)) {
            typedb.databases().create(database);

            Set<Variable> keyVariables;

            loadComplexRecursionExample(typedb, database);

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA)) {
                try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    ConceptMap answer = tx.query().match(inferenceQuery).next();
                    keyVariables = generateKeyConstraints(answer.concepts());
                }
            }

            Set<BoundVariable> expectedVariables = TypeQL.parsePattern(
                    "$currency \"GBP\" isa currency;\n" +
                            "$transaction has currency \"GBP\";\n"
            ).asConjunction().variables().collect(Collectors.toSet());

            assertEquals(expectedVariables, keyVariables);
        }
    }

    /**
     * Create a set of variables that will query for the keys of the concepts given in the map. Attributes given in
     * the map are simply queried for by their own type and value.
     * @param varMap variable map of concepts
     * @return Variables that check for the keys of the given concepts
     */
    public static Set<Variable> generateKeyConstraints(Map<Identifier.Variable.Retrievable, ? extends Concept> varMap) {
        LinkedHashSet<ThingVariable.Thing> variables = new LinkedHashSet<>();

        for (Map.Entry<Identifier.Variable.Retrievable, ? extends Concept> entry : varMap.entrySet()) {
            Identifier.Variable.Retrievable var = entry.getKey();
            Concept concept = entry.getValue();

            if (concept.isAttribute()) {
                Attribute attribute = concept.asAttribute();
                String attributeTypeName = attribute.getType().getLabel().toString();
                ThingVariable.Thing variable = TypeQL.var(var.toString()).isa(attributeTypeName);

                if (attribute.isString()) {
                    variable.has(attributeTypeName, attribute.asString().getValue());
                } else if (attribute.isDouble()) {
                    variable.has(attributeTypeName, attribute.asDouble().getValue());
                } else if (attribute.isLong()) {
                    variable.has(attributeTypeName, attribute.asLong().getValue());
                } else if (attribute.isDateTime()) {
                    variable.has(attributeTypeName, attribute.asDateTime().getValue());
                } else if (attribute.isBoolean()) {
                    variable.has(attributeTypeName, attribute.asBoolean().getValue());
                }
                variables.add(variable);

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
                throw new ResolutionTestingException("Presently we only handle queries concerning Things, not Types");
            }
        }
        return variables;
    }
}