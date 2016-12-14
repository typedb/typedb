package ai.grakn;

import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

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

    //------------------------------------- Meta Types ----------------------------------
    /**
     * Get the root of all Types.
     *
     * @return The meta type -> type.
     */
    Type getMetaType();

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

}
