package ai.grakn;

import ai.grakn.concept.Concept;
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
     *
     * @return A read only tinkerpop traversal for manually traversing the graph
     */
    GraphTraversal<Vertex, Vertex> getTinkerTraversal();

    //------------------------------------- Meta Types ----------------------------------
    /**
     *
     * @return The meta type -> type. The root of all Types.
     */
    Type getMetaType();

    /**
     *
     * @return The meta relation type -> relation-type. The root of all Relation Types.
     */
    Type getMetaRelationType();

    /**
     *
     * @return The meta role type -> role-type. The root of all the Role Types.
     */
    Type getMetaRoleType();

    /**
     *
     * @return The meta resource type -> resource-type. The root of all the Resource Types.
     */
    Type getMetaResourceType();

    /**
     *
     * @return The meta entity type -> entity-type. The root of all the Entity Types.
     */
    Type getMetaEntityType();

    /**
     *
     * @return The meta rule type -> rule-type. The root of all the Rule Types.
     */
    Type getMetaRuleType();

    /**
     *
     * @return The meta rule -> inference-rule.
     */
    RuleType getMetaRuleInference();

    /**
     *
     * @return The meta rule -> constraint-rule.
     */
    RuleType getMetaRuleConstraint();
}
