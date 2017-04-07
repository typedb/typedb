/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graph.admin;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Set;

/**
 * Admin interface for {@link GraknGraph}.
 *
 * @author Filipe Teixeira
 */
public interface GraknAdmin {

    /**
     *
     * @param vertex A vertex which contains properties necessary to build a concept from.
     * @param <T> The type of the concept being built
     * @return A concept built using the provided vertex
     */
    <T extends Concept> T buildConcept(Vertex vertex);

    /**
     * Utility function to get a read-only Tinkerpop traversal.
     *
     * @return A read-only Tinkerpop traversal for manually traversing the graph
     */
    GraphTraversal<Vertex, Vertex> getTinkerTraversal();

    /**
     * A flag to check if batch loading is enabled and consistency checks are switched off
     *
     * @return true if batch loading is enabled
     */
    boolean isBatchLoadingEnabled();

    //------------------------------------- Meta Types ----------------------------------
    /**
     * Get the root of all Types.
     *
     * @return The meta type -> type.
     */
    Type getMetaConcept();

    /**
     * Get the root of all Relation Types.
     *
     * @return The meta relation type -> relation-type.
     */
    RelationType getMetaRelationType();

    /**
     * Get the root of all the Role Types.
     *
     * @return The meta role type -> role-type.
     */
    RoleType getMetaRoleType();

    /**
     * Get the root of all the Resource Types.
     *
     * @return The meta resource type -> resource-type.
     */
    ResourceType getMetaResourceType();

    /**
     * Get the root of all the Entity Types.
     *
     * @return The meta entity type -> entity-type.
     */
    EntityType getMetaEntityType();

    /**
     * Get the root of all Rule Types;
     *
     * @return The meta rule type -> rule-type.
     */
    RuleType getMetaRuleType();

    /**
     * Get the root of all inference rules.
     *
     * @return The meta rule -> inference-rule.
     */
    RuleType getMetaRuleInference();

    /**
     * Get the root of all constraint rules.
     *
     * @return The meta rule -> constraint-rule.
     */
    RuleType getMetaRuleConstraint();

    //------------------------------------- Admin Specific Operations ----------------------------------

    /**
     * Commits the graph and adds concepts for post processing directly to the cache bypassing the REST API.
     *
     * @param conceptCache The concept Cache to store concepts in for processing later
     * @throws GraknValidationException when the graph does not conform to the object concept
     */
    void commit(ConceptCache conceptCache) throws GraknValidationException;

    /**
     * Clears the graph and empties the provided cache.
     *
     * @param conceptCache The concept Cache to store concepts in for processing later
     */
    void clear(ConceptCache conceptCache);

    /**
     * Commits to the graph without submitting any commit logs.
     *
     * @throws GraknValidationException when the graph does not conform to the object concept
     */
    void commitNoLogs() throws GraknValidationException;

    /**
     * Merges the provided duplicate castings.
     *
     * @param castingVertexIds The vertex Ids of the duplicate castings
     * @return if castings were merged and a commit is required.
     */
    boolean fixDuplicateCastings(String index, Set<ConceptId> castingVertexIds);

    /**
     * Merges the provided duplicate resources
     *
     * @param resourceVertexIds The resource vertex ids which need to be merged.
     * @return True if a commit is required.
     */
    boolean fixDuplicateResources(String index, Set<ConceptId> resourceVertexIds);

    /**
     * Updates the counts of all the types
     *
     * @param typeCounts The types and the changes to put on their counts
     */
    void updateTypeCounts(Map<TypeLabel, Long> typeCounts);

    /**
     *
     * @param key The concept property tp search by.
     * @param value The value of the concept
     * @return A concept with the matching key and value
     */
    <T extends Concept> T  getConcept(Schema.ConceptProperty key, String value);
}
