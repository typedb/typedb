/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.admin;

import ai.grakn.GraknTx;
import ai.grakn.QueryRunner;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;

import javax.annotation.CheckReturnValue;
import java.util.stream.Stream;

/**
 * Admin interface for {@link GraknTx}.
 *
 * @author Filipe Teixeira
 */
public interface GraknAdmin {


    //------------------------------------- Meta Types ----------------------------------
    /**
     * Get the root of all Types.
     *
     * @return The meta type -> type.
     */
    @CheckReturnValue
    Type getMetaConcept();

    /**
     * Get the root of all {@link RelationshipType}.
     *
     * @return The meta relation type -> relation-type.
     */
    @CheckReturnValue
    RelationshipType getMetaRelationType();

    /**
     * Get the root of all the {@link Role}.
     *
     * @return The meta role type -> role-type.
     */
    @CheckReturnValue
    Role getMetaRole();

    /**
     * Get the root of all the {@link AttributeType}.
     *
     * @return The meta resource type -> resource-type.
     */
    @CheckReturnValue
    AttributeType getMetaAttributeType();

    /**
     * Get the root of all the Entity Types.
     *
     * @return The meta entity type -> entity-type.
     */
    @CheckReturnValue
    EntityType getMetaEntityType();

    /**
     * Get the root of all {@link Rule}s;
     *
     * @return The meta {@link Rule}
     */
    @CheckReturnValue
    Rule getMetaRule();

    //------------------------------------- Admin Specific Operations ----------------------------------

    /**
     * Get all super-concepts of the given {@link SchemaConcept} including itself and including the meta-type
     * {@link ai.grakn.util.Schema.MetaSchema#THING}.
     *
     * <p>
     *     If you want a more precise type that will exclude {@link ai.grakn.util.Schema.MetaSchema#THING}, use
     *     {@link SchemaConcept#sups()}.
     * </p>
     */
    @CheckReturnValue
    Stream<SchemaConcept> sups(SchemaConcept schemaConcept);

    /**
     * Immediately closes the session and deletes the graph.
     * Should be used with caution as this will invalidate any pending transactions
     */
    void delete();

    QueryRunner queryRunner();
}
