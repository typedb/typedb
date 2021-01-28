/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.logic.resolvable.Unifier;
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

    public ConceptMap conceptMap() {
        return conceptMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnswerState that = (AnswerState) o;
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

            public Optional<DownstreamVars.Unified> toDownstreamVars(Unifier unifier) {
                Optional<ConceptMap> unified = unifier.unify(conceptMap());
                return unified.map(conceptMap -> new DownstreamVars.Unified(this, conceptMap, unifier));
            }

            @Override
            public String toString() {
                return "AnswerState.UpstreamVars.Initial{" +
                        "conceptMap=" + conceptMap() +
                        '}';
            }
        }

        public static class Derived extends AnswerState {

            private final Initial initial;

            Derived(ConceptMap derivedAnswer, @Nullable UpstreamVars.Initial source) {
                super(derivedAnswer);
                this.initial = source;
            }

            public ConceptMap withInitial() {
                HashMap<Reference.Name, Concept> withInitial = new HashMap<>(conceptMap().concepts());
                if (initial != null) {
                   withInitial.putAll(initial.conceptMap().concepts());
                }
                return new ConceptMap(withInitial);
            }

            @Override
            public String toString() {
                return "AnswerState.UpstreamVars.Derived{" +
                        "derivedAnswer=" + conceptMap() +
                        "initial=" + initial +
                        '}';
            }
        }
    }

    public static abstract class DownstreamVars extends AnswerState {

        DownstreamVars(ConceptMap conceptMap) {
            super(conceptMap);
        }

        public boolean isRoot() { return false; }

        public boolean isMapped() { return false; }

        public boolean isUnified() { return false; }

        public Root asRoot() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Root.class));
        }

        public AnswerState.DownstreamVars.Mapped asMapped() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AnswerState.DownstreamVars.Mapped.class));
        }

        public AnswerState.DownstreamVars.Unified asUnified() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AnswerState.DownstreamVars.Unified.class));
        }

        public static class Root extends DownstreamVars {

            Root(ConceptMap conceptMap) {
                super(conceptMap);
            }

            public static Root create() {
                // This is the entry-point answer state for the request received by the root resolver
                return new Root(new ConceptMap());
            }

            public UpstreamVars.Derived aggregateToUpstream(ConceptMap conceptMap) {
                if (conceptMap == null) return null;
                if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                return new UpstreamVars.Derived(new ConceptMap(conceptMap.concepts()), null);
            }

            @Override
            public boolean isRoot() { return true; }

            @Override
            public Root asRoot() { return this; }


            @Override
            public String toString() {
                return "AnswerState.DownstreamVars.Root{" +
                        "conceptMap=" + conceptMap() +
                        '}';
            }
        }

        public static class Mapped extends DownstreamVars {

            private final UpstreamVars.Initial initial;
            private final Mapping mapping;

            Mapped(UpstreamVars.Initial initial, Mapping mapping) {
                super(mapping.transform(initial.conceptMap()));
                this.initial = initial;
                this.mapping = mapping;
            }

            public UpstreamVars.Derived mapToUpstream(ConceptMap additionalConcepts) {
                return new UpstreamVars.Derived(new ConceptMap(mapping.unTransform(additionalConcepts).concepts()), initial);
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
                Mapped mapped = (Mapped) o;
                return Objects.equals(initial, mapped.initial) &&
                        Objects.equals(mapping, mapped.mapping);
            }

            @Override
            public int hashCode() {
                return Objects.hash(initial, mapping);
            }

            @Override
            public String toString() {
                return "AnswerState.DownstreamVars.Mapped{" +
                        "initial=" + conceptMap() +
                        "mapping=" + mapping +
                        '}';
            }
        }

        public static class Unified extends DownstreamVars {

            private final UpstreamVars.Initial initial;
            private final Unifier unifier;

            Unified(UpstreamVars.Initial initial, ConceptMap unifiedInitial, Unifier unifier) {
                super(unifiedInitial);
                this.initial = initial;
                this.unifier = unifier;
            }

            public Optional<UpstreamVars.Derived> unifyToUpstream(Map<Identifier, Concept> identifiedConcepts) {
                Optional<ConceptMap> reversed = unifier.unUnify(identifiedConcepts);
                return reversed.map(map -> new UpstreamVars.Derived(new ConceptMap(map.concepts()), initial));
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
                Unified unified = (Unified) o;
                return Objects.equals(initial, unified.initial) &&
                        Objects.equals(unifier, unified.unifier);
            }

            @Override
            public int hashCode() {
                return Objects.hash(initial, unifier);
            }

            @Override
            public String toString() {
                return "AnswerState.DownstreamVars.Unified{" +
                        "initial=" + conceptMap() +
                        "unifier=" + unifier +
                        '}';
            }
        }
    }
}
