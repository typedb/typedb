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
 */

package grakn.core.reasoner.resolution.answer;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.transformer.Mapping;
import grakn.core.logic.transformer.Unifier;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class AnswerState {
    private final ConceptMap conceptMap;

    AnswerState(ConceptMap conceptMap) {
        this.conceptMap = conceptMap;
    }

    protected ConceptMap conceptMap() {
        return conceptMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AnswerState that = (AnswerState) o;
        return conceptMap.equals(that.conceptMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conceptMap);
    }

    public static class UpstreamVars {

        public static class Initial extends AnswerState {

            Initial(ConceptMap conceptMap) {
                super(conceptMap);
            }

            public static Initial of(ConceptMap conceptMap) {
                return new Initial(conceptMap);
            }

            public DownstreamVars.Mapped toDownstreamVars(Mapping mapping) {
                return new DownstreamVars.Mapped(this, mapping);
            }

            public DownstreamVars.Unified toDownstreamVars(Unifier unifier) {
                return new DownstreamVars.Unified(this, unifier);
            }
        }

        public static class Derived extends AnswerState {

            private final Initial source;

            Derived(ConceptMap derivedAnswer, @Nullable UpstreamVars.Initial source) {
                super(derivedAnswer);
                this.source = source;
            }
        }
    }

    public static abstract class DownstreamVars extends AnswerState {

        DownstreamVars(ConceptMap conceptMap) {
            super(conceptMap);
        }

        public boolean isEmpty() { return false; }

        public boolean isMapped() { return false; }

        public boolean isUnified() { return false; }

        public AnswerState.DownstreamVars.Empty asEmpty() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AnswerState.DownstreamVars.Empty.class));
        }

        public AnswerState.DownstreamVars.Mapped asMapped() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AnswerState.DownstreamVars.Mapped.class));
        }

        public AnswerState.DownstreamVars.Unified asUnified() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AnswerState.DownstreamVars.Unified.class));
        }

        public static class Empty extends DownstreamVars {

            Empty(ConceptMap conceptMap) {
                super(conceptMap);
            }

            public static Empty create() {
                // This is the entry-point answer state for the request received by the root resolver
                return new Empty(new ConceptMap());
            }

            public UpstreamVars.Derived aggregateToUpstream(ConceptMap conceptMap) {
                if (conceptMap == null) return null;
                if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                return new UpstreamVars.Derived(new ConceptMap(conceptMap().concepts()), null);
            }

            @Override
            public boolean isEmpty() { return true; }

            @Override
            public Empty asEmpty() { return this; }
        }

        public static class Mapped extends DownstreamVars {

            private final UpstreamVars.Initial initial;
            private final Mapping mapping;

            Mapped(UpstreamVars.Initial initial, Mapping mapping) {
                super(mapping.transform(initial.conceptMap()));
                this.initial = initial;
                this.mapping = mapping;
            }

            public UpstreamVars.Derived aggregateToUpstream(ConceptMap additionalConcepts) {
                Map<Reference.Name, Concept> merged = new HashMap<>(additionalConcepts.concepts());
                merged.putAll(conceptMap().concepts());
                return new UpstreamVars.Derived(mapping.unTransform(new ConceptMap(merged)), initial);
            }

            @Override
            public boolean isMapped() { return true; }

            @Override
            public Mapped asMapped() { return this; }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                if (!super.equals(o)) return false;
                final Mapped mapped = (Mapped) o;
                return Objects.equals(initial, mapped.initial) &&
                        Objects.equals(mapping, mapped.mapping);
            }

            @Override
            public int hashCode() {
                return Objects.hash(initial, mapping);
            }
        }

        public static class Unified extends DownstreamVars {

            private final UpstreamVars.Initial initial;
            private final Unifier unifier;

            Unified(UpstreamVars.Initial initial, Unifier unifier) {
                super(unifier.unify(initial.conceptMap()));
                this.initial = initial;
                this.unifier = unifier;

            }

            public Optional<UpstreamVars.Derived> aggregateToUpstream(Map<Identifier, Concept> identifiedConcepts) {
                Optional<ConceptMap> reversed = unifier.unUnify(identifiedConcepts);
                if (reversed.isPresent()) {
                    HashMap<Reference.Name, Concept> merged = new HashMap<>(reversed.get().concepts());
                    merged.putAll(conceptMap().concepts());
                    return Optional.of(new UpstreamVars.Derived(new ConceptMap(merged), initial));
                } else {
                    return Optional.empty();
                }
            }

            @Override
            public boolean isUnified() { return true; }

            @Override
            public Unified asUnified() { return this; }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                if (!super.equals(o)) return false;
                final Unified unified = (Unified) o;
                return Objects.equals(initial, unified.initial) &&
                        Objects.equals(unifier, unified.unifier);
            }

            @Override
            public int hashCode() {
                return Objects.hash(initial, unifier);
            }
        }
    }
}
