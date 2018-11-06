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

package ai.grakn;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.kb.admin.GraknAdmin;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * <p>
 *     A {@link GraknTx} holding a database transaction
 * </p>
 *
 * <p>
 *     This is produced by {@link Grakn#session(String, String)} and allows the user to construct and perform
 *     basic look ups to the knowledge base. This also allows the execution of Graql queries.
 * </p>
 *
 * @author fppt
 *
 */
public interface GraknTx extends AutoCloseable{

    //------------------------------------- Concept Construction ----------------------------------
    /**
     * Create a new {@link EntityType} with super-type {@code entity}, or return a pre-existing {@link EntityType},
     * with the specified label.
     *
     * @param label A unique label for the {@link EntityType}
     * @return A new or existing {@link EntityType} with the provided label
     *
     * @throws GraknTxOperationException if the graph is closed
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
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link EntityType}.
     */
    EntityType putEntityType(Label label);

    /**
     * Create a new non-unique {@link AttributeType} with super-type {@code resource}, or return a pre-existing
     * non-unique {@link AttributeType}, with the specified label and data type.
     *
     * @param label A unique label for the {@link AttributeType}
     * @param dataType The data type of the {@link AttributeType}.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing {@link AttributeType} with the provided label and data type.
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link AttributeType}.
     * @throws GraknTxOperationException if the {@param label} is already in use by an existing {@link AttributeType} which is
     *                          unique or has a different datatype.
     */
    default <V> AttributeType<V> putAttributeType(String label, AttributeType.DataType<V> dataType) {
        return putAttributeType(Label.of(label), dataType);
    }

    /**
     * Create a new non-unique {@link AttributeType} with super-type {@code resource}, or return a pre-existing
     * non-unique {@link AttributeType}, with the specified label and data type.
     *
     * @param label A unique label for the {@link AttributeType}
     * @param dataType The data type of the {@link AttributeType}.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing {@link AttributeType} with the provided label and data type.
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link AttributeType}.
     * @throws GraknTxOperationException if the {@param label} is already in use by an existing {@link AttributeType} which is
     *                          unique or has a different datatype.
     */
    <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType);

    /**
     * Create a {@link Rule} with super-type {@code rule}, or return a pre-existing {@link Rule}, with the
     * specified label.
     *
     * @param label A unique label for the {@link Rule}
     * @param when A string representing the when part of the {@link Rule}
     * @param then A string representing the then part of the {@link Rule}
     * @return new or existing {@link Rule} with the provided label.
     *
     * @throws GraknTxOperationException if the graph is closed
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
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link Rule}.
     */
    Rule putRule(Label label, Pattern when, Pattern then);

    /**
     * Create a {@link RelationshipType} with super-type {@code relation}, or return a pre-existing {@link RelationshipType},
     * with the specified label.
     *
     * @param label A unique label for the {@link RelationshipType}
     * @return A new or existing {@link RelationshipType} with the provided label.
     *
     * @throws GraknTxOperationException if the graph is closed
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
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link RelationshipType}.
     */
    RelationshipType putRelationshipType(Label label);

    /**
     * Create a {@link Role} with super-type {@code role}, or return a pre-existing {@link Role}, with the
     * specified label.
     *
     * @param label A unique label for the {@link Role}
     * @return new or existing {@link Role} with the provided Id.
     *
     * @throws GraknTxOperationException if the graph is closed
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
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-{@link Role}.
     */
    Role putRole(Label label);

    //------------------------------------- Concept Lookup ----------------------------------
    /**
     * Get the {@link Concept} with identifier provided, if it exists.
     *
     * @param id A unique identifier for the {@link Concept} in the graph.
     * @return The {@link Concept} with the provided id or null if no such {@link Concept} exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws ClassCastException if the concept is not an instance of {@link T}
     */
    @CheckReturnValue
    @Nullable
    <T extends Concept> T getConcept(ConceptId id);

    /**
     * Get the {@link SchemaConcept} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link SchemaConcept} in the graph.
     * @return The {@link SchemaConcept} with the provided label or null if no such {@link SchemaConcept} exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws ClassCastException if the type is not an instance of {@link T}
     */
    @CheckReturnValue
    @Nullable
    <T extends SchemaConcept> T getSchemaConcept(Label label);

    /**
     * Get the {@link Type} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link Type} in the graph.
     * @return The {@link Type} with the provided label or null if no such {@link Type} exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     * @throws ClassCastException if the type is not an instance of {@link T}
     */
    @CheckReturnValue
    @Nullable
    <T extends Type> T getType(Label label);

    /**
     * Get all {@link Attribute} holding the value provided, if they exist.
     *
     * @param value A value which an {@link Attribute} in the graph may be holding.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The {@link Attribute}s holding the provided value or an empty collection if no such {@link Attribute} exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     */
    @CheckReturnValue
    <V> Collection<Attribute<V>> getAttributesByValue(V value);

    /**
     * Get the Entity Type with the label provided, if it exists.
     *
     * @param label A unique label which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided label or null if no such Entity Type exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    EntityType getEntityType(String label);

    /**
     * Get the {@link RelationshipType} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link RelationshipType} in the graph.
     * @return The {@link RelationshipType} with the provided label or null if no such {@link RelationshipType} exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    RelationshipType getRelationshipType(String label);

    /**
     * Get the {@link AttributeType} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link AttributeType} in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The {@link AttributeType} with the provided label or null if no such {@link AttributeType} exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    <V> AttributeType<V> getAttributeType(String label);

    /**
     * Get the Role Type with the label provided, if it exists.
     *
     * @param label A unique label which identifies the Role Type in the graph.
     * @return The Role Type  with the provided label or null if no such Role Type exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    Role getRole(String label);

    /**
     * Get the {@link Rule} with the label provided, if it exists.
     *
     * @param label A unique label which identifies the {@link Rule} in the graph.
     * @return The {@link Rule} with the provided label or null if no such Rule Type exists.
     *
     * @throws GraknTxOperationException if the graph is closed
     */
    @CheckReturnValue
    @Nullable
    Rule getRule(String label);

    //------------------------------------- Utilities ----------------------------------
    /**
     * Returns access to the low-level details of the graph via GraknAdmin
     * @see GraknAdmin
     *
     * @return The admin interface which allows you to access more low level details of the graph.
     */
    @CheckReturnValue
    GraknAdmin admin();

    /**
     * Utility function used to check if the current transaction on the graph is a read only transaction
     *
     * @return true if the current transaction is read only
     */
    @CheckReturnValue
    GraknTxType txType();

    /**
     * Returns the {@link GraknSession} which was used to create this {@link GraknTx}
     * @return the owner {@link GraknSession}
     */
    GraknSession session();

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

    // TODO: what does this do when the graph is closed?
    /**
     * Returns a QueryBuilder
     *
     * @return returns a query builder to allow for the creation of graql queries
     * @see QueryBuilder
     */
    @CheckReturnValue
    QueryBuilder graql();

    /**
     * Closes the current transaction. Rendering this graph unusable. You must use the {@link GraknSession} to
     * get a new open transaction.
     */
    void close();

    /**
     * Reverts any changes done to the graph and closes the transaction. You must use the {@link GraknSession} to
     * get a new open transaction.
     */
    default void abort(){
        close();
    }

    /**
     * Commits any changes to the graph and closes the transaction. You must use the {@link GraknSession} to
     * get a new open transaction.
     *
     */
    void commit();

}
