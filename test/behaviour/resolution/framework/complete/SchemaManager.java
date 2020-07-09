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

package grakn.core.test.behaviour.resolution.framework.complete;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.common.ResolutionConstraintException;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.test.behaviour.resolution.framework.common.Utils.loadGqlFile;


public class SchemaManager {
    private static final Path SCHEMA_PATH = Paths.get("test", "behaviour", "resolution", "framework", "complete", "completion_schema.gql").toAbsolutePath();

    private static HashSet<String> EXCLUDED_ENTITY_TYPES = new HashSet<String>() {
        {
            add("entity");
        }
    };

    private static HashSet<String> EXCLUDED_RELATION_TYPES = new HashSet<String>() {
        {
            add("relation");
            add("var-property");
            add("isa-property");
            add("has-attribute-property");
            add("relation-property");
            add("resolution");
        }
    };

    private static HashSet<String> EXCLUDED_ATTRIBUTE_TYPES = new HashSet<String>() {
        {
            add("attribute");
            add("label");
            add("rule-label");
            add("type-label");
            add("role-label");
        }
    };

    public static void undefineAllRules(Session session) {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            Set<String> ruleLabels = getAllRules(tx).stream().map(rule -> rule.label().toString()).collect(Collectors.toSet());
            for (String ruleLabel : ruleLabels) {
                tx.execute(Graql.undefine(Graql.type(ruleLabel).sub("rule")));
            }
            tx.commit();
        }
    }

    public static Set<Rule> getAllRules(Transaction tx) {
        return tx.stream(Graql.match(Graql.var("r").sub("rule")).get()).map(ans -> ans.get("r").asRule()).filter(rule -> !rule.label().toString().equals("rule")).collect(Collectors.toSet());
    }

    public static void addResolutionSchema(Session session) {
        try {
            loadGqlFile(session, SCHEMA_PATH);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Role getRole(Transaction tx, String roleLabel) {
        GraqlGet roleQuery = Graql.match(Graql.var("x").sub(roleLabel)).get();
        return tx.execute(roleQuery).get(0).get("x").asRole();
    }

    public static void connectResolutionSchema(Session session) {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            Role instanceRole = getRole(tx, "instance");
            Role ownerRole = getRole(tx, "owner");
            Role roleplayerRole = getRole(tx, "roleplayer");
            Role relRole = getRole(tx, "rel");

            RelationType attrPropRel = tx.execute(Graql.match(Graql.var("x").sub("has-attribute-property")).get()).get(0).get("x").asRelationType();
            
            GraqlGet typesToConnectQuery = Graql.match(
                    Graql.var("x").sub("thing")
            ).get();
            tx.stream(typesToConnectQuery).map(ans -> ans.get("x").asType()).forEach(type -> {
                if (type.isAttributeType()) {
                    if (!EXCLUDED_ATTRIBUTE_TYPES.contains(type.label().toString())) {
                        attrPropRel.has((AttributeType) type);
                        type.plays(instanceRole);
                        type.plays(ownerRole);
                        type.plays(roleplayerRole);
                    }
                } else if (type.isEntityType()) {
                    if (!EXCLUDED_ENTITY_TYPES.contains(type.label().toString())) {
                        type.plays(instanceRole);
                        type.plays(ownerRole);
                        type.plays(roleplayerRole);
                    }

                } else if (type.isRelationType()) {
                    if (!EXCLUDED_RELATION_TYPES.contains(type.label().toString())) {
                        type.plays(instanceRole);
                        type.plays(ownerRole);
                        type.plays(roleplayerRole);
                        type.plays(relRole);
                    }
                }
            });
            tx.commit();
        }
    }

    private static boolean typeIsMemberOfCompletionSchema(Type type) {
        if (type.isAttributeType()) {
            return EXCLUDED_ATTRIBUTE_TYPES.contains(type.label().toString());

        } else if (type.isEntityType()) {
            return EXCLUDED_ENTITY_TYPES.contains(type.label().toString());

        } else if (type.isRelationType()) {
            return EXCLUDED_RELATION_TYPES.contains(type.label().toString());
        } else {
            throw new UnsupportedOperationException("Type not recognised");
        }
    }

    /**
     * Filters out answers that contain any Thing instance that is derived from the completion schema.
     * @param answerStream stream of answers to filter
     * @return filtered stream of answers
     */
    public static Stream<ConceptMap> filterCompletionSchema(Stream<ConceptMap> answerStream) {
        return answerStream.filter(a -> a.map().values().stream().noneMatch(concept -> typeIsMemberOfCompletionSchema(concept.asThing().type())));
    }


    public static void enforceAllTypesHaveKeys(Session session) {
        Transaction tx = session.transaction(Transaction.Type.READ);

        GraqlGet instancesQuery = Graql.match(Graql.var("x").sub("thing"),
                Graql.not(Graql.var("x").sub("attribute")),
                Graql.not(Graql.var("x").type("entity")),
                Graql.not(Graql.var("x").type("relation")),
                Graql.not(Graql.var("x").type("thing"))
        ).get();
        Stream<ConceptMap> answers = tx.stream(instancesQuery);

        answers.forEach(ans -> {
            Type type = ans.get("x").asType();
            if (!type.isAbstract() && type.keys().collect(Collectors.toSet()).isEmpty()) {
                throw new ResolutionConstraintException(String.format("Type \"%s\" doesn't have any keys declared. Keys are required " +
                        "for all entity types and relation types for resolution testing", type.label().toString()));
            }
        });
        tx.close();
    }
}
