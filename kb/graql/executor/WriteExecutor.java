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

package grakn.core.kb.graql.executor;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Optional;
import java.util.stream.Stream;

public interface WriteExecutor {
    void toDelete(Concept concept);

    /**
     * Return a ConceptBuilder for given Variable. This can be used to provide information for how to create
     * the concept that the variable represents.
     * This method is expected to be called from implementations of
     * VarProperty#insert(Variable), provided they return the given Variable in the
     * response to PropertyExecutor#producedVars().
     * For example, a property may call {@code executor.builder(var).isa(type);} in order to provide a type for a var.
     *
     * @throws GraqlSemanticException if the concept in question has already been created
     */
    ConceptBuilder getBuilder(Variable var);

    /**
     * Return a ConceptBuilder for given Variable. This can be used to provide information for how to create
     * the concept that the variable represents.
     * This method is expected to be called from implementations of
     * VarProperty#insert(Variable), provided they include the given Variable in
     * their PropertyExecutor#producedVars().
     * For example, a property may call {@code executor.builder(var).isa(type);} in order to provide a type for a var.
     * If the concept has already been created, this will return empty.
     */
    Optional<ConceptBuilder> tryBuilder(Variable var);

    /**
     * Return a Concept for a given Variable.
     * This method is expected to be called from implementations of
     * VarProperty#insert(Variable), provided they include the given Variable in
     * their PropertyExecutor#requiredVars().
     */
    Concept getConcept(Variable var);

    Stream<ConceptMap> write(ConceptMap preExisting);
    Stream<ConceptMap> write();

    boolean isConceptDefined(Variable var);

    Statement printableRepresentation(Variable var);
}
