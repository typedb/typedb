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

import grakn.core.graql.answer.Answer;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.server.exception.PropertyNotUniqueException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.keyspace.Keyspace;

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
     * This class is used to describe how a transaction on {@link Transaction} should behave.
     * When producing a graph using a {@link Session} one of the following enums must be provided:
     * READ - A read only transaction. If you attempt to mutate the graph with such a transaction an exception will be thrown.
     * WRITE - A transaction which allows you to mutate the graph.
     * BATCH - A transaction which allows mutations to be performed more quickly but disables some consistency checks.
     */
    enum Type {
        READ(0),  //Read only transaction where mutations to the graph are prohibited
        WRITE(1), //Write transaction where the graph can be mutated
        BATCH(2); //Batch transaction which enables faster writes by switching off some consistency checks

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

    default <T extends Answer> List<T> execute(Query<T> query) {
        return execute(query, true);
    }

    default <T extends Answer> List<T> execute(Query<T> query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    default <T extends Answer> Stream<T> stream(Query<T> query) {
        return stream(query, true);
    }

    default <T extends Answer> Stream<T> stream(Query<T> query, boolean infer) {
        if (query instanceof DefineQuery) {
            return (Stream<T>) executor(infer).run((DefineQuery) query);
        } else if (query instanceof UndefineQuery) {
            return (Stream<T>) executor(infer).run((UndefineQuery) query);
        } else if (query instanceof InsertQuery) {
            return (Stream<T>) executor(infer).run((InsertQuery) query);
        } else if (query instanceof DeleteQuery) {
            return (Stream<T>) executor(infer).run((DeleteQuery) query);
        } else if (query instanceof GetQuery) {
            return (Stream<T>) executor(infer).run((GetQuery) query);
        } else if (query instanceof AggregateQuery<?>) {
            return (Stream<T>) executor(infer).run((AggregateQuery<?>) query);
        } else if (query instanceof ComputeQuery<?>) {
            return (Stream<T>) executor(infer).run((ComputeQuery<?>) query);
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
    default grakn.core.graql.concept.Type getMetaConcept() {
        return getSchemaConcept(Schema.MetaSchema.THING.getLabel());
    }

    /**
     * Get the root of all {@link RelationshipType}.
     *
     * @return The meta relation type -> relation-type.
     */
    @CheckReturnValue
    default RelationshipType getMetaRelationType() {
        return getSchemaConcept(Schema.MetaSchema.RELATIONSHIP.getLabel());
    }

    /**
     * Get the root of all the {@link Role}.
     *
     * @return The meta role type -> role-type.
     */
    @CheckReturnValue
    default Role getMetaRole() {
        return getSchemaConcept(Schema.MetaSchema.ROLE.getLabel());
    }

    /**
     * Get the root of all the {@link AttributeType}.
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
     * Get the root of all {@link Rule}s;
     *
     * @return The meta {@link Rule}
     */
    @CheckReturnValue
    default Rule getMetaRule() {
        return getSchemaConcept(Schema.MetaSchema.RULE.getLabel());
    }

    //------------------------------------- Admin Specific Operations ----------------------------------

    /**
     * Get all super-concepts of the given {@link SchemaConcept} including itself and including the meta-type
     * {@link Schema.MetaSchema#THING}.
     *
     * <p>
     * If you want a more precise type that will exclude {@link Schema.MetaSchema#THING}, use
     * {@link SchemaConcept#sups()}.
     * </p>
     */
    @CheckReturnValue
    Stream<SchemaConcept> sups(SchemaConcept schemaConcept);


    QueryExecutor executor();

    QueryExecutor executor(boolean infer);

    //------------------------------------- Concept Construction ----------------------------------

    /**
     * Create a new {@link EntityType} with super-type {@code entity}, or return a pre-existing {@link EntityType},
     * with the specified label.
     *
     * @param label A unique label for the {@link EntityType}
     * @return A new or existing {@link EntityType} with the provided label
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link EntityType}.
     */
    default EntityType putEntityType(String label) {
        return putEntityType(Label.of(label));
    }

    /**
     * Create a new {@link EntityType} with super-type {@code entity}, or return a pre-existing {@link EntityType},
     * with the specified label.
     *
     * @param label A unique label for the {@link EntityType}
     * @return A new or existing {@link EntityType} with the provided label
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link EntityType}.
     */
    EntityType putEntityType(Label label);

    /**
     * Create a new non-unique {@link AttributeType} with super-type {@code resource}, or return a pre-existing
     * non-unique {@link AttributeType}, with the specified label and data type.
     *
     * @param label    A unique label for the {@link AttributeType}
     * @param dataType The data type of the {@link AttributeType}.
     *                 Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V>      The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *                 This should match the parameter type
     * @return A new or existing {@link AttributeType} with the provided label and data type.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link AttributeType}.
     * @throws TransactionException       if the {@param label} is already in use by an existing {@link AttributeType} which is
     *                                    unique or has a different datatype.
     */
    default <V> AttributeType<V> putAttributeType(String label, AttributeType.DataType<V> dataType) {
        return putAttributeType(Label.of(label), dataType);
    }

    /**
     * Create a new non-unique {@link AttributeType} with super-type {@code resource}, or return a pre-existing
     * non-unique {@link AttributeType}, with the specified label and data type.
     *
     * @param label    A unique label for the {@link AttributeType}
     * @param dataType The data type of the {@link AttributeType}.
     *                 Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V>      The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *                 This should match the parameter type
     * @return A new or existing {@link AttributeType} with the provided label and data type.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link AttributeType}.
     * @throws TransactionException       if the {@param label} is already in use by an existing {@link AttributeType} which is
     *                                    unique or has a different datatype.
     */
    <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType);

    /**
     * Create a {@link Rule} with super-type {@code rule}, or return a pre-existing {@link Rule}, with the
     * specified label.
     *
     * @param label A unique label for the {@link Rule}
     * @param when  A string representing the when part of the {@link Rule}
     * @param then  A string representing the then part of the {@link Rule}
     * @return new or existing {@link Rule} with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link Rule}.
     */
    default Rule putRule(String label, Pattern when, Pattern then) {
        return putRule(Label.of(label), when, then);
    }

    /**
     * Create a {@link Rule} with super-type {@code rule}, or return a pre-existing {@link Rule}, with the
     * specified label.
     *
     * @param label A unique label for the {@link Rule}
     * @return new or existing {@link Rule} with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link Rule}.
     */
    Rule putRule(Label label, Pattern when, Pattern then);

    /**
     * Create a {@link RelationshipType} with super-type {@code relation}, or return a pre-existing {@link RelationshipType},
     * with the specified label.
     *
     * @param label A unique label for the {@link RelationshipType}
     * @return A new or existing {@link RelationshipType} with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link RelationshipType}.
     */
    default RelationshipType putRelationshipType(String label) {
        return putRelationshipType(Label.of(label));
    }

    /**
     * Create a {@link RelationshipType} with super-type {@code relation}, or return a pre-existing {@link RelationshipType},
     * with the specified label.
     *
     * @param label A unique label for the {@link RelationshipType}
     * @return A new or existing {@link RelationshipType} with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link RelationshipType}.
     */
    RelationshipType putRelationshipType(Label label);

    /**
     * Create a {@link Role} with super-type {@code role}, or return a pre-existing {@link Role}, with the
     * specified label.
     *
     * @param label A unique label for the {@link Role}
     * @return new or existing {@link Role} with the provided Id.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link Role}.
     */
    default Role putRole(String label) {
        return putRole(Label.of(label));
    }

    /**
     * Create a {@link Role} with super-type {@code role}, or return a pre-existing {@link Role}, with the
     * specified label.
     *
     * @param label A unique label for the {@link Role}
     * @return new or existing {@link Role} with the provided Id.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link Role}.
     */
    Role putRole(Label label);

    //------------------------------------- Concept Lookup ----------------------------------

    /**
     * Get the {@link Concept} with identifier provided, if it exists.
     *
     * @param id A unique identifier for the {@link Concept} in the graph.
     * @return The {@link Concept} with the provided id or null if no such {@link Concept} exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the concept is not an instance of {@link T}
     */
    @CheckReturnValue
    @Nullable
    <T extends Concept> T getConcept(ConceptId id);

    /**
     * Get the {@link SchemaConcept} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link SchemaConcept} in the graph.
     * @return The {@link SchemaConcept} with the provided label or null if no such {@link SchemaConcept} exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of {@link T}
     */
    @CheckReturnValue
    @Nullable
    <T extends SchemaConcept> T getSchemaConcept(Label label);

    /**
     * Get the {@link grakn.core.graql.concept.Type} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link grakn.core.graql.concept.Type} in the graph.
     * @return The {@link grakn.core.graql.concept.Type} with the provided label or null if no such {@link grakn.core.graql.concept.Type} exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of {@link T}
     */
    @CheckReturnValue
    @Nullable
    <T extends grakn.core.graql.concept.Type> T getType(Label label);

    /**
     * Get all {@link Attribute} holding the value provided, if they exist.
     *
     * @param value A value which an {@link Attribute} in the graph may be holding.
     * @param <V>   The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The {@link Attribute}s holding the provided value or an empty collection if no such {@link Attribute} exists.
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
     * Get the {@link RelationshipType} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link RelationshipType} in the graph.
     * @return The {@link RelationshipType} with the provided label or null if no such {@link RelationshipType} exists.
     * @throws TransactionException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    RelationshipType getRelationshipType(String label);

    /**
     * Get the {@link AttributeType} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link AttributeType} in the graph.
     * @param <V>   The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The {@link AttributeType} with the provided label or null if no such {@link AttributeType} exists.
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
     * Get the {@link Rule} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link Rule} in the graph.
     * @return The {@link Rule} with the provided label or null if no such Rule Type exists.
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
    Type txType();

    /**
     * Returns the {@link Session} which was used to create this {@link Transaction}
     *
     * @return the owner {@link Session}
     */
    Session session();

    /**
     * Utility function to get {@link Keyspace} of the knowledge base.
     *
     * @return The {@link Keyspace} of the knowledge base.
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
     * Closes the current transaction. Rendering this graph unusable. You must use the {@link Session} to
     * get a new open transaction.
     */
    void close();

    /**
     * Reverts any changes done to the graph and closes the transaction. You must use the {@link Session} to
     * get a new open transaction.
     */
    default void abort() {
        close();
    }

    /**
     * Commits any changes to the graph and closes the transaction. You must use the {@link Session} to
     * get a new open transaction.
     */
    void commit();

}
