/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server;

import grakn.core.concept.answer.Answer;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.Label;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.server.exception.PropertyNotUniqueException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.keyspace.Keyspace;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlCompute;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlQuery;
import graql.lang.query.GraqlUndefine;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Transaction holding a database transaction
 * This is produced by by calling Session.transaction() and allows the user to construct and perform
 * basic look ups to the knowledge base. This also allows the execution of Graql queries.
 */
public interface Transaction extends AutoCloseable {

    /**
     * An enum that determines the type of Grakn Transaction.
     * This class is used to describe how a transaction on Transaction should behave.
     * When producing a graph using a Session one of the following enums must be provided:
     * READ - A read only transaction. If you attempt to mutate the graph with such a transaction an exception will be thrown.
     * WRITE - A transaction which allows you to mutate the graph.
     * BATCH - A transaction which allows mutations to be performed more quickly but disables some consistency checks.
     */
    enum Type {
        READ(0),  //Read only transaction where mutations to the graph are prohibited
        WRITE(1); //Write transaction where the graph can be mutated

        private final int type;

        Type(int type) {
            this.type = type;
        }

        public int getId() {
            return type;
        }

        @Override
        public String toString() {
            return this.name();
        }

        public static Type of(int value) {
            for (Type t : Type.values()) {
                if (t.type == value) return t;
            }
            return null;
        }
    }

    // Define Query

    default List<ConceptMap> execute(GraqlDefine query) {
        return stream(query).collect(Collectors.toList());
    }

    Stream<ConceptMap> stream(GraqlDefine query);

    // Undefine Query

    default List<ConceptMap> execute(GraqlUndefine query) {
        return stream(query).collect(Collectors.toList());
    }

    Stream<ConceptMap> stream(GraqlUndefine query);

    // Insert Query

    default List<ConceptMap> execute(GraqlInsert query) {
        return execute(query, true);
    }

    default List<ConceptMap> execute(GraqlInsert query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<ConceptMap> stream(GraqlInsert query) {
        return stream(query, true);
    }

    Stream<ConceptMap> stream(GraqlInsert query, boolean infer);

    // Delete Query

    default List<ConceptSet> execute(GraqlDelete query) {
        return execute(query, true);
    }

    default List<ConceptSet> execute(GraqlDelete query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<ConceptSet> stream(GraqlDelete query) {
        return stream(query, true);
    }

    Stream<ConceptSet> stream(GraqlDelete query, boolean infer);

    // Get Query

    default List<ConceptMap> execute(GraqlGet query) {
        return execute(query, true);
    }

    default List<ConceptMap> execute(GraqlGet query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<ConceptMap> stream(GraqlGet query) {
        return stream(query, true);
    }

    Stream<ConceptMap> stream(GraqlGet query, boolean infer);

    // Aggregate Query

    default List<Numeric> execute(GraqlGet.Aggregate query) {
        return execute(query, true);
    }

    default List<Numeric> execute(GraqlGet.Aggregate query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<Numeric> stream(GraqlGet.Aggregate query) {
        return stream(query, true);
    }

    Stream<Numeric> stream(GraqlGet.Aggregate query, boolean infer);

    // Group Query

    default List<AnswerGroup<ConceptMap>> execute(GraqlGet.Group query) {
        return execute(query, true);
    }

    default List<AnswerGroup<ConceptMap>> execute(GraqlGet.Group query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<AnswerGroup<ConceptMap>> stream(GraqlGet.Group query) {
        return stream(query, true);
    }

    Stream<AnswerGroup<ConceptMap>> stream(GraqlGet.Group query, boolean infer);

    // Group Aggregate Query

    default List<AnswerGroup<Numeric>> execute(GraqlGet.Group.Aggregate query) {
        return execute(query, true);
    }

    default List<AnswerGroup<Numeric>> execute(GraqlGet.Group.Aggregate query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<AnswerGroup<Numeric>> stream(GraqlGet.Group.Aggregate query) {
        return stream(query, true);
    }

    Stream<AnswerGroup<Numeric>> stream(GraqlGet.Group.Aggregate query, boolean infer);

    // Compute Query

    default List<Numeric> execute(GraqlCompute.Statistics query) {
        return execute(query, true);
    }

    default List<Numeric> execute(GraqlCompute.Statistics query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<Numeric> stream(GraqlCompute.Statistics query) {
        return stream(query, true);
    }

    Stream<Numeric> stream(GraqlCompute.Statistics query, boolean infer);

    default List<ConceptList> execute(GraqlCompute.Path query) {
        return execute(query, true);
    }

    default List<ConceptList> execute(GraqlCompute.Path query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<ConceptList> stream(GraqlCompute.Path query) {
        return stream(query, true);
    }

    Stream<ConceptList> stream(GraqlCompute.Path query, boolean infer);

    default List<ConceptSetMeasure> execute(GraqlCompute.Centrality query) {
        return execute(query, true);
    }

    default List<ConceptSetMeasure> execute(GraqlCompute.Centrality query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<ConceptSetMeasure> stream(GraqlCompute.Centrality query) {
        return stream(query, true);
    }

    Stream<ConceptSetMeasure> stream(GraqlCompute.Centrality query, boolean infer);

    default List<ConceptSet> execute(GraqlCompute.Cluster query) {
        return execute(query, true);
    }

    default List<ConceptSet> execute(GraqlCompute.Cluster query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<ConceptSet> stream(GraqlCompute.Cluster query) {
        return stream(query, true);
    }

    Stream<ConceptSet> stream(GraqlCompute.Cluster query, boolean infer);

    // Generic Query

    default List<? extends Answer> execute(GraqlQuery query) {
        return execute(query, true);
    }

    default List<? extends Answer> execute(GraqlQuery query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default Stream<? extends Answer> stream(GraqlQuery query) {
        return stream(query, true);
    }

    default Stream<? extends Answer> stream(GraqlQuery query, boolean infer) {
        if (query instanceof GraqlDefine) {
            return stream((GraqlDefine) query);

        } else if (query instanceof GraqlUndefine) {
            return stream((GraqlUndefine) query);

        } else if (query instanceof GraqlInsert) {
            return stream((GraqlInsert) query, infer);

        } else if (query instanceof GraqlDelete) {
            return stream((GraqlDelete) query, infer);

        } else if (query instanceof GraqlGet) {
            return stream((GraqlGet) query, infer);

        } else if (query instanceof GraqlGet.Aggregate) {
            return stream((GraqlGet.Aggregate) query, infer);

        } else if (query instanceof GraqlGet.Group.Aggregate) {
            return stream((GraqlGet.Group.Aggregate) query, infer);

        } else if (query instanceof GraqlGet.Group) {
            return stream((GraqlGet.Group) query, infer);

        } else if (query instanceof GraqlCompute.Statistics) {
            return stream((GraqlCompute.Statistics) query, infer);

        } else if (query instanceof GraqlCompute.Path) {
            return stream((GraqlCompute.Path) query, infer);

        } else if (query instanceof GraqlCompute.Centrality) {
            return stream((GraqlCompute.Centrality) query, infer);

        } else if (query instanceof GraqlCompute.Cluster) {
            return stream((GraqlCompute.Cluster) query, infer);

        } else {
            throw new IllegalArgumentException("Unrecognised Query object");
        }
    }

    //------------------------------------- Meta Types ----------------------------------

    /**
     * Get the root of all Types.
     *
     * @return The meta type -> type.
     */
    @CheckReturnValue
    default grakn.core.concept.type.Type getMetaConcept() {
        return getSchemaConcept(Schema.MetaSchema.THING.getLabel());
    }

    /**
     * Get the root of all RelationType.
     *
     * @return The meta relation type -> relation-type.
     */
    @CheckReturnValue
    default RelationType getMetaRelationType() {
        return getSchemaConcept(Schema.MetaSchema.RELATIONSHIP.getLabel());
    }

    /**
     * Get the root of all the Role.
     *
     * @return The meta role type -> role-type.
     */
    @CheckReturnValue
    default Role getMetaRole() {
        return getSchemaConcept(Schema.MetaSchema.ROLE.getLabel());
    }

    /**
     * Get the root of all the AttributeType.
     *
     * @return The meta resource type -> resource-type.
     */
    @CheckReturnValue
    default AttributeType getMetaAttributeType() {
        return getSchemaConcept(Schema.MetaSchema.ATTRIBUTE.getLabel());
    }

    /**
     * Get the root of all the Entity Types.
     *
     * @return The meta entity type -> entity-type.
     */
    @CheckReturnValue
    default EntityType getMetaEntityType() {
        return getSchemaConcept(Schema.MetaSchema.ENTITY.getLabel());
    }

    /**
     * Get the root of all Rules;
     *
     * @return The meta Rule
     */
    @CheckReturnValue
    default Rule getMetaRule() {
        return getSchemaConcept(Schema.MetaSchema.RULE.getLabel());
    }

    //------------------------------------- Admin Specific Operations ----------------------------------

    /**
     * Get all super-concepts of the given SchemaConcept including itself and including the meta-type
     * Schema.MetaSchema#THING.
     * <p>
     * If you want a more precise type that will exclude Schema.MetaSchema#THING, use
     * SchemaConcept#sups().
     */
    @CheckReturnValue
    Stream<SchemaConcept> sups(SchemaConcept schemaConcept);

    //------------------------------------- Concept Construction ----------------------------------

    /**
     * Create a new EntityType with super-type {@code entity}, or return a pre-existing EntityType,
     * with the specified label.
     *
     * @param label A unique label for the EntityType
     * @return A new or existing EntityType with the provided label
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-EntityType.
     */
    default EntityType putEntityType(String label) {
        return putEntityType(Label.of(label));
    }

    /**
     * Create a new EntityType with super-type {@code entity}, or return a pre-existing EntityType,
     * with the specified label.
     *
     * @param label A unique label for the EntityType
     * @return A new or existing EntityType with the provided label
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-EntityType.
     */
    EntityType putEntityType(Label label);

    /**
     * Create a new non-unique AttributeType with super-type {@code resource}, or return a pre-existing
     * non-unique AttributeType, with the specified label and data type.
     *
     * @param label    A unique label for the AttributeType
     * @param dataType The data type of the AttributeType.
     *                 Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V>      The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *                 This should match the parameter type
     * @return A new or existing AttributeType with the provided label and data type.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-AttributeType.
     * @throws TransactionException       if the {@param label} is already in use by an existing AttributeType which is
     *                                    unique or has a different datatype.
     */
    default <V> AttributeType<V> putAttributeType(String label, AttributeType.DataType<V> dataType) {
        return putAttributeType(Label.of(label), dataType);
    }

    /**
     * Create a new non-unique AttributeType with super-type {@code resource}, or return a pre-existing
     * non-unique AttributeType, with the specified label and data type.
     *
     * @param label    A unique label for the AttributeType
     * @param dataType The data type of the AttributeType.
     *                 Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V>      The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *                 This should match the parameter type
     * @return A new or existing AttributeType with the provided label and data type.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-AttributeType.
     * @throws TransactionException       if the {@param label} is already in use by an existing AttributeType which is
     *                                    unique or has a different datatype.
     */
    <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType);

    /**
     * Create a Rule with super-type {@code rule}, or return a pre-existing Rule, with the
     * specified label.
     *
     * @param label A unique label for the Rule
     * @param when  A string representing the when part of the Rule
     * @param then  A string representing the then part of the Rule
     * @return new or existing Rule with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Rule.
     */
    default Rule putRule(String label, Pattern when, Pattern then) {
        return putRule(Label.of(label), when, then);
    }

    /**
     * Create a Rule with super-type {@code rule}, or return a pre-existing Rule, with the
     * specified label.
     *
     * @param label A unique label for the Rule
     * @return new or existing Rule with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Rule.
     */
    Rule putRule(Label label, Pattern when, Pattern then);

    /**
     * Create a RelationType with super-type {@code relation}, or return a pre-existing RelationType,
     * with the specified label.
     *
     * @param label A unique label for the RelationType
     * @return A new or existing RelationType with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-RelationType.
     */
    default RelationType putRelationType(String label) {
        return putRelationType(Label.of(label));
    }

    /**
     * Create a RelationType with super-type {@code relation}, or return a pre-existing RelationType,
     * with the specified label.
     *
     * @param label A unique label for the RelationType
     * @return A new or existing RelationType with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-RelationType.
     */
    RelationType putRelationType(Label label);

    /**
     * Create a Role with super-type {@code role}, or return a pre-existing Role, with the
     * specified label.
     *
     * @param label A unique label for the Role
     * @return new or existing Role with the provided Id.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Role.
     */
    default Role putRole(String label) {
        return putRole(Label.of(label));
    }

    /**
     * Create a Role with super-type {@code role}, or return a pre-existing Role, with the
     * specified label.
     *
     * @param label A unique label for the Role
     * @return new or existing Role with the provided Id.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Role.
     */
    Role putRole(Label label);

    //------------------------------------- Concept Lookup ----------------------------------

    /**
     * Get the Concept with identifier provided, if it exists.
     *
     * @param id A unique identifier for the Concept in the graph.
     * @return The Concept with the provided id or null if no such Concept exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the concept is not an instance of T
     */
    @CheckReturnValue
    @Nullable
    <T extends Concept> T getConcept(ConceptId id);

    /**
     * Get the SchemaConcept with the label provided, if it exists.
     *
     * @param label A unique label which identifies the SchemaConcept in the graph.
     * @return The SchemaConcept with the provided label or null if no such SchemaConcept exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of T
     */
    @CheckReturnValue
    @Nullable
    <T extends SchemaConcept> T getSchemaConcept(Label label);

    /**
     * Get the grakn.core.concept.type.Type with the label provided, if it exists.
     *
     * @param label A unique label which identifies the grakn.core.concept.type.Type in the graph.
     * @return The grakn.core.concept.type.Type with the provided label or null if no such grakn.core.concept.type.Type exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of T
     */
    @CheckReturnValue
    @Nullable
    <T extends grakn.core.concept.type.Type> T getType(Label label);

    /**
     * Get all Attribute holding the value provided, if they exist.
     *
     * @param value A value which an Attribute in the graph may be holding.
     * @param <V>   The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Attributes holding the provided value or an empty collection if no such Attribute exists.
     * @throws TransactionException if the graph is closed
     */
    @CheckReturnValue
    <V> Collection<Attribute<V>> getAttributesByValue(V value);

    /**
     * Get the Entity Type with the label provided, if it exists.
     *
     * @param label A unique label which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided label or null if no such Entity Type exists.
     * @throws TransactionException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    EntityType getEntityType(String label);

    /**
     * Get the RelationType with the label provided, if it exists.
     *
     * @param label A unique label which identifies the RelationType in the graph.
     * @return The RelationType with the provided label or null if no such RelationType exists.
     * @throws TransactionException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    RelationType getRelationType(String label);

    /**
     * Get the AttributeType with the label provided, if it exists.
     *
     * @param label A unique label which identifies the AttributeType in the graph.
     * @param <V>   The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The AttributeType with the provided label or null if no such AttributeType exists.
     * @throws TransactionException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    <V> AttributeType<V> getAttributeType(String label);

    /**
     * Get the Role Type with the label provided, if it exists.
     *
     * @param label A unique label which identifies the Role Type in the graph.
     * @return The Role Type  with the provided label or null if no such Role Type exists.
     * @throws TransactionException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    Role getRole(String label);

    /**
     * Get the Rule with the label provided, if it exists.
     *
     * @param label A unique label which identifies the Rule in the graph.
     * @return The Rule with the provided label or null if no such Rule Type exists.
     * @throws TransactionException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    Rule getRule(String label);

    /**
     * Utility function used to check if the current transaction on the graph is a read only transaction
     *
     * @return true if the current transaction is read only
     */
    @CheckReturnValue
    Type type();

    /**
     * Returns the Session which was used to create this Transaction
     *
     * @return the owner Session
     */
    Session session();

    /**
     * Utility function to get Keyspace of the knowledge base.
     *
     * @return The Keyspace of the knowledge base.
     */
    @CheckReturnValue
    default Keyspace keyspace() {
        return session().keyspace();
    }

    /**
     * Utility function to determine whether the graph has been closed.
     *
     * @return True if the graph has been closed
     */
    @CheckReturnValue
    boolean isClosed();

    /**
     * Closes the current transaction. Rendering this graph unusable. You must use the Session to
     * get a new open transaction.
     */
    void close();

    /**
     * Reverts any changes done to the graph and closes the transaction. You must use the Session to
     * get a new open transaction.
     */
    default void abort() {
        close();
    }

    /**
     * Commits any changes to the graph and closes the transaction. You must use the Session to
     * get a new open transaction.
     */
    void commit();

}
