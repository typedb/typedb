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
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.INSTANCE;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.OWNED;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.OWNER;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.REL;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.ROLEPLAYER;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.HAS_ATTRIBUTE_PROPERTY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.ISA_PROPERTY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.RELATION_PROPERTY;


public class SchemaManager {
    private static final Path SCHEMA_PATH = Paths.get("test", "behaviour", "resolution", "framework", "complete", "completion_schema.gql").toAbsolutePath();

    public enum CompletionSchemaType {
        // Relations
        VAR_PROPERTY("var-property"),
        ISA_PROPERTY("isa-property"),
        HAS_ATTRIBUTE_PROPERTY("has-attribute-property"),
        RELATION_PROPERTY("relation-property"),
        RESOLUTION("resolution"),

        // Attributes
        LABEL("label"),
        RULE_LABEL("rule-label"),
        TYPE_LABEL("type-label"),
        ROLE_LABEL("role-label"),
        INFERRED("inferred");

        private final String name;

        CompletionSchemaType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public enum CompletionSchemaRole {
        INSTANCE("instance"),
        OWNER("owner"),
        OWNED("owned"),
        ROLEPLAYER("roleplayer"),
        REL("rel");

        private final String label;

        CompletionSchemaRole(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }

        @Override
        public String toString() {
            return label;
        }
    }

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

    private static void loadGqlFile(Session session, Path... gqlPath) throws IOException {
        for (Path path : gqlPath) {
            String query = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().match(TypeQL.parseQuery(query).asMatch()).toList();
                tx.commit();
            }
        }
    }

    private static RoleType getRole(Transaction tx, String roleLabel) {
        TypeQLMatch roleQuery = TypeQL.match(TypeQL.var("x").type(roleLabel));
        return tx.query().match(roleQuery).next().get("x").asRoleType();
    }

    public static void connectCompletionSchema(Session session) {
        // TODO: Check that the user hasn't defined anything that conflicts with the CompletionSchema
        try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            RoleType instanceRole = getRole(tx, ISA_PROPERTY + ":" + INSTANCE);
            RoleType ownerRole = getRole(tx, HAS_ATTRIBUTE_PROPERTY + ":" + OWNER);
            RoleType ownedRole = getRole(tx, HAS_ATTRIBUTE_PROPERTY + ":" + OWNED);
            RoleType roleplayerRole = getRole(tx, RELATION_PROPERTY + ":" + ROLEPLAYER);
            RoleType relationRole = getRole(tx, RELATION_PROPERTY + ":" + REL);
            Set<String> completionSchemaTypes =
                    iterate(CompletionSchemaType.values()).map(CompletionSchemaType::toString).toSet();

            TypeQLMatch typesToConnectQuery = TypeQL.match(TypeQL.var("x").sub(TypeQLToken.Type.THING.toString()));

            tx.query().match(typesToConnectQuery).map(ans -> ans.get("x").asThingType()).forEachRemaining(type -> {
                if (completionSchemaTypes.contains(type.getLabel().toString())) return;  // TODO: Why do we need to do this? Surely we should throw if anything
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
