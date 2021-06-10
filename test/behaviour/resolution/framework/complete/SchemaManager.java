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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.complete;

import com.vaticle.typedb.core.TypeDB.Session;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.Utils.loadGqlFile;


public class SchemaManager {
    private static final Path SCHEMA_PATH = Paths.get("test", "behaviour", "resolution", "framework", "complete", "completion_schema.gql").toAbsolutePath();

    // TODO: Use Enums
    public static final HashSet<String> COMPLETION_SCHEMA_TYPES = new HashSet<String>() {
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
        try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            for (String ruleLabel : iterate(getAllRules(tx)).map(Rule::getLabel).toSet()) {
                tx.query().undefine(TypeQL.undefine(TypeQL.rule(ruleLabel).asTypeVariable()));
            }
            tx.commit();
        }
    }

    public static Set<Rule> getAllRules(Transaction tx) {
        return tx.logic().rules().toSet();
    }

    public static void addCompletionSchema(Session session) {
        try {
            loadGqlFile(session, SCHEMA_PATH);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static RoleType getRole(Transaction tx, String roleLabel) {
        TypeQLMatch roleQuery = TypeQL.match(TypeQL.var("x").type(roleLabel));
        return tx.query().match(roleQuery).next().get("x").asRoleType();
    }

    public static void connectCompletionSchema(Session session) {
        try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            RoleType instanceRole = getRole(tx, "isa-property:instance");
            RoleType ownerRole = getRole(tx, "has-attribute-property:owner");
            RoleType ownedRole = getRole(tx, "has-attribute-property:owned");
            RoleType roleplayerRole = getRole(tx, "relation-property:roleplayer");
            RoleType relationRole = getRole(tx, "relation-property:rel");
            
            TypeQLMatch typesToConnectQuery = TypeQL.match(TypeQL.var("x").sub("thing"));
            tx.query().match(typesToConnectQuery).map(ans -> ans.get("x").asThingType()).forEachRemaining(type -> {
                if (COMPLETION_SCHEMA_TYPES.contains(type.getLabel().toString())) return;
                type.setPlays(instanceRole);
                type.setPlays(ownerRole);
                type.setPlays(roleplayerRole);
                if (type.isAttributeType()) {
                    type.setPlays(ownedRole);
                } else if (type.isRelationType()) {
                    type.setPlays(relationRole);
                }
            });
            tx.commit();
        }
    }
}
