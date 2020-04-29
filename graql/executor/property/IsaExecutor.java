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

package grakn.core.graql.executor.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.exception.GraqlQueryException;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.Graql;
import graql.lang.property.IsaProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class IsaExecutor implements PropertyExecutor.Insertable, PropertyExecutor.Deletable {

    private static final Logger LOG = LoggerFactory.getLogger(IsaExecutor.class);

    private final Variable var;
    private final IsaProperty property;

    IsaExecutor(Variable var, IsaProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        Variable directTypeVar = new Variable();
        if (!property.isExplicit()) {
            return ImmutableSet.of(
                    EquivalentFragmentSets.isa(property, var, directTypeVar, true),
                    EquivalentFragmentSets.sub(property, directTypeVar, property.type().var())
            );
        } else {
            return ImmutableSet.of(
                    EquivalentFragmentSets.isa(property, var, property.type().var(), true)
            );
        }
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertIsa());
    }

    @Override
    public Set<Writer> deleteExecutors() {
        return ImmutableSet.of(new DeleteIsa());
    }

    private class InsertIsa implements PropertyExecutor.Writer {

        @Override
        public Variable var() {
            return var;
        }

        @Override
        public VarProperty property() {
            return property;
        }

        @Override
        public Set<Variable> requiredVars() {
            return ImmutableSet.of(property.type().var());
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of(var);
        }


        @Override
        public void execute(WriteExecutor executor) {
            Type type = executor.getConcept(property.type().var()).asType();
            if (executor.isConceptDefined(var)) {
                Concept concept = executor.getConcept(var); // retrieve the existing concept
                // we silently "allow" redefining concepts, while actually doing a no-op, as long as the type hasn't changed
                if (type.subs().map(SchemaConcept::label).noneMatch(label -> label.equals(concept.asThing().type().label()))) {
                    //downcasting is bad
                    throw GraqlSemanticException.conceptDowncast(concept.asThing().type(), type);
                }
                //upcasting we silently accept
            } else {
                executor.getBuilder(var).isa(type);
            }
        }
    }


    private class DeleteIsa implements PropertyExecutor.Writer {

        @Override
        public Variable var() {
            return var;
        }

        @Override
        public VarProperty property() {
            return property;
        }

        @Override
        public Set<Variable> requiredVars() {
            return ImmutableSet.of();
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of();
        }

        @Override
        public TiebreakDeletionOrdering ordering(WriteExecutor executor) {
            Concept concept = executor.getConcept(var);
            if (concept.isRelation()) {
                if (concept.asThing().type().isImplicit()) {
                    return TiebreakDeletionOrdering.EDGE;
                } else {
                    return TiebreakDeletionOrdering.RELATION_INSTANCE;
                }
            } else {
                return TiebreakDeletionOrdering.NON_RELATION_INSTANCE;
            }
        }

        @Override
        public void execute(WriteExecutor executor) {
            if (executor.getConcept(var).isSchemaConcept()) {
                throw GraqlSemanticException.deleteSchemaConcept(executor.getConcept(var).asSchemaConcept());
            }
            Thing concept = executor.getConcept(var).asThing();

            Label expectedType;
            Statement expectedTypeStatement = property.type();
            if (expectedTypeStatement.getType().isPresent()) {
                expectedType = Label.of(expectedTypeStatement.getType().get());
            } else {
                Variable typeVar = expectedTypeStatement.var();
                expectedType = executor.getConcept(typeVar).asType().label();
            }

            // ensure that the concept is an instance of the required type by the delete
            if (property.isExplicit()) {
                if (!concept.type().label().equals(expectedType)) {
                    throw GraqlSemanticException.cannotDeleteInstanceIncorrectType(var, concept, expectedType);
                }
            } else {
                // using THING always ok if not using isa!
                // otherwise we have to check all parent types - if none match, then we throw
                if (!expectedType.equals(Label.of(Graql.Token.Type.THING.toString())) &&
                        concept.type().sups().noneMatch(sub -> sub.label().equals(expectedType))) {
                    throw GraqlSemanticException.cannotDeleteInstanceIncorrectTypeOrSubtype(var, concept, expectedType);
                }
            }

            // do this after type checks to ensure that we throw and abort if something is the wrong type
            if (concept.isDeleted()) {
                LOG.trace("Skipping deletion of concept " + concept + ", is already deleted");
                return;
            }
            concept.delete();
        }
    }
}
