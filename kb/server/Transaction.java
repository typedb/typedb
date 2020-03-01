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

package grakn.core.kb.server;

import grakn.core.concept.answer.Answer;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Explanation;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.Void;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.structure.GraknElementException;
import grakn.core.kb.concept.structure.PropertyNotUniqueException;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.kb.server.keyspace.Keyspace;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlCompute;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlQuery;
import graql.lang.query.GraqlUndefine;
import graql.lang.query.MatchClause;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public interface Transaction extends AutoCloseable {
    void open(Type type);

    Session session();

    Keyspace keyspace();

    List<ConceptMap> execute(GraqlDefine query);

    Stream<ConceptMap> stream(GraqlDefine query);

    List<ConceptMap> execute(GraqlUndefine query);

    Stream<ConceptMap> stream(GraqlUndefine query);

    List<ConceptMap> execute(GraqlInsert query, boolean infer);

    List<ConceptMap> execute(GraqlInsert query);

    Stream<ConceptMap> stream(GraqlInsert query);

    Stream<ConceptMap> stream(GraqlInsert query, boolean infer);

    List<Void> execute(GraqlDelete query);

    List<Void> execute(GraqlDelete query, boolean infer);

    Stream<Void> stream(GraqlDelete query);

    Stream<Void> stream(GraqlDelete query, boolean infer);

    List<ConceptMap> execute(GraqlGet query);

    List<ConceptMap> execute(GraqlGet query, boolean infer);

    Stream<ConceptMap> stream(GraqlGet query);

    Stream<ConceptMap> stream(GraqlGet query, boolean infer);

    List<Numeric> execute(GraqlGet.Aggregate query);

    List<Numeric> execute(GraqlGet.Aggregate query, boolean infer);

    Stream<Numeric> stream(GraqlGet.Aggregate query);

    Stream<Numeric> stream(GraqlGet.Aggregate query, boolean infer);

    List<AnswerGroup<ConceptMap>> execute(GraqlGet.Group query);

    List<AnswerGroup<ConceptMap>> execute(GraqlGet.Group query, boolean infer);

    Stream<AnswerGroup<ConceptMap>> stream(GraqlGet.Group query);

    Stream<AnswerGroup<ConceptMap>> stream(GraqlGet.Group query, boolean infer);

    List<AnswerGroup<Numeric>> execute(GraqlGet.Group.Aggregate query);

    List<AnswerGroup<Numeric>> execute(GraqlGet.Group.Aggregate query, boolean infer);

    Stream<AnswerGroup<Numeric>> stream(GraqlGet.Group.Aggregate query);

    Stream<AnswerGroup<Numeric>> stream(GraqlGet.Group.Aggregate query, boolean infer);

    List<Numeric> execute(GraqlCompute.Statistics query);

    Stream<Numeric> stream(GraqlCompute.Statistics query);

    List<ConceptList> execute(GraqlCompute.Path query);

    Stream<ConceptList> stream(GraqlCompute.Path query);

    List<ConceptSetMeasure> execute(GraqlCompute.Centrality query);

    Stream<ConceptSetMeasure> stream(GraqlCompute.Centrality query);

    List<ConceptSet> execute(GraqlCompute.Cluster query);

    Stream<ConceptSet> stream(GraqlCompute.Cluster query);

    List<? extends Answer> execute(GraqlQuery query);

    List<? extends Answer> execute(GraqlQuery query, boolean infer);

    Stream<? extends Answer> stream(GraqlQuery query);

    Stream<? extends Answer> stream(GraqlQuery query, boolean infer);

    boolean isOpen();

    Type type();

    Stream<SchemaConcept> sups(SchemaConcept schemaConcept);

    /**
     * @param label A unique label for the EntityType
     * @return A new or existing EntityType with the provided label
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-EntityType.
     */
    EntityType putEntityType(Label label);

    EntityType putEntityType(String label);

    /**
     * @param label A unique label for the RelationType
     * @return A new or existing RelationType with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-RelationType.
     */
    RelationType putRelationType(Label label);

    RelationType putRelationType(String label);

    /**
     * @param label A unique label for the Role
     * @return new or existing Role with the provided Id.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Role.
     */
    Role putRole(Label label);

    Role putRole(String label);

    /**
     * @param label    A unique label for the AttributeType
     * @param dataType The data type of the AttributeType.
     *                 Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V>
     * @return A new or existing AttributeType with the provided label and data type.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-AttributeType.
     * @throws GraknElementException if the {@param label} is already in use by an existing AttributeType which is
     *                                    unique or has a different datatype.
     */
    @SuppressWarnings("unchecked")
    <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType);

    <V> AttributeType<V> putAttributeType(String label, AttributeType.DataType<V> dataType);

    /**
     * @param label A unique label for the Rule
     * @param when
     * @param then
     * @return new or existing Rule with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Rule.
     */
    Rule putRule(Label label, Pattern when, Pattern then);

    Rule putRule(String label, Pattern when, Pattern then);

    /**
     * @param id  A unique identifier for the Concept in the graph.
     * @param <T>
     * @return The Concept with the provided id or null if no such Concept exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the concept is not an instance of T
     */
    <T extends Concept> T getConcept(ConceptId id);

    /**
     * Get the root of all Types.
     *
     * @return The meta type -> type.
     */
    @CheckReturnValue
    grakn.core.kb.concept.api.Type getMetaConcept();

    /**
     * Get the root of all RelationType.
     *
     * @return The meta relation type -> relation-type.
     */
    @CheckReturnValue
    RelationType getMetaRelationType();

    /**
     * Get the root of all the Role.
     *
     * @return The meta role type -> role-type.
     */
    @CheckReturnValue
    Role getMetaRole();

    /**
     * Get the root of all the AttributeType.
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
     * Get the root of all Rules;
     *
     * @return The meta Rule
     */
    @CheckReturnValue
    Rule getMetaRule();

    /**
     * @param value A value which an Attribute in the graph may be holding.
     * @param <V>
     * @return The Attributes holding the provided value or an empty collection if no such Attribute exists.
     * @throws TransactionException if the graph is closed
     */
    <V> Collection<Attribute<V>> getAttributesByValue(V value);

    /**
     * @param label A unique label which identifies the SchemaConcept in the graph.
     * @param <T>
     * @return The SchemaConcept with the provided label or null if no such SchemaConcept exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of T
     */
    <T extends SchemaConcept> T getSchemaConcept(Label label);

    /**
     * @param label A unique label which identifies the Type in the graph.
     * @param <T>
     * @return The Type with the provided label or null if no such Type exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of T
     */
    <T extends grakn.core.kb.concept.api.Type> T getType(Label label);

    /**
     * @param label A unique label which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided label or null if no such Entity Type exists.
     * @throws TransactionException if the graph is closed
     */
    EntityType getEntityType(String label);

    /**
     * @param label A unique label which identifies the RelationType in the graph.
     * @return The RelationType with the provided label or null if no such RelationType exists.
     * @throws TransactionException if the graph is closed
     */
    RelationType getRelationType(String label);

    /**
     * @param label A unique label which identifies the AttributeType in the graph.
     * @param <V>
     * @return The AttributeType with the provided label or null if no such AttributeType exists.
     * @throws TransactionException if the graph is closed
     */
    <V> AttributeType<V> getAttributeType(String label);

    /**
     * @param label A unique label which identifies the Role Type in the graph.
     * @return The Role Type  with the provided label or null if no such Role Type exists.
     * @throws TransactionException if the graph is closed
     */
    Role getRole(String label);

    /**
     * @param label A unique label which identifies the Rule in the graph.
     * @return The Rule with the provided label or null if no such Rule Type exists.
     * @throws TransactionException if the graph is closed
     */
    Rule getRule(String label);

    Explanation explanation(Pattern queryPattern);

    @Override
    void close();

    void close(String closeMethod);

    /**
     * Commits and closes the transaction
     *
     * @throws InvalidKBException if graph does not comply with the grakn validation rules
     */
    void commit() throws InvalidKBException;

    Stream<ConceptMap> stream(MatchClause matchClause);

    Stream<ConceptMap> stream(MatchClause matchClause, boolean infer);

    List<ConceptMap> execute(MatchClause matchClause);

    List<ConceptMap> execute(MatchClause matchClause, boolean infer);


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

        public int id() {
            return type;
        }

        @Override
        public String toString() {
            return this.name();
        }

        public static Transaction.Type of(int value) {
            for (Transaction.Type t : Transaction.Type.values()) {
                if (t.type == value) return t;
            }
            return null;
        }
    }
}
