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
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
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

    private static HashSet<String> EXCLUDED_TYPES = new HashSet<String>() {
        {
            // Concept
            add("thing");

            // Entity
            add("entity");

            // Relation
            add("relation");
            add("var-property");
            add("isa-property");
            add("has-attribute-property");
            add("relation-property");
            add("resolution");

            // Attribute
            add("attribute");
            add("label");
            add("rule-label");
            add("type-label");
            add("role-label");
            add("inferred");
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
            Role ownedRole = getRole(tx, "owned");
            Role roleplayerRole = getRole(tx, "roleplayer");
            Role relRole = getRole(tx, "rel");
            
            GraqlGet typesToConnectQuery = Graql.match(
                    Graql.var("x").sub("thing")
            ).get();
            tx.stream(typesToConnectQuery).map(ans -> ans.get("x").asType()).forEach(type -> {
                if (EXCLUDED_TYPES.contains(type.label().toString())) {
                    return;
                }
                type.plays(instanceRole);
                type.plays(ownerRole);
                type.plays(roleplayerRole);
                if (type.isAttributeType()) {
                    type.plays(ownedRole);
                } else if (type.isRelationType()) {
                    type.plays(relRole);
                }
            });
            tx.commit();
        }
    }

    private static boolean typeIsMemberOfCompletionSchema(Type type) {
        if (type.isEntityType() || type.isAttributeType() || type.isRelationType()) {
            return EXCLUDED_TYPES.contains(type.label().toString());
        }
        throw new UnsupportedOperationException("Type not recognised");
    }

    /**
     * Filters out answers that contain any Thing instance that is derived from the completion schema.
     * @param answerStream stream of answers to filter
     * @return filtered stream of answers
     */
    public static Stream<ConceptMap> filterCompletionSchema(Stream<ConceptMap> answerStream) {
        return answerStream.filter(a -> a.map().values().stream().noneMatch(concept -> typeIsMemberOfCompletionSchema(concept.asThing().type())));
    }
}
