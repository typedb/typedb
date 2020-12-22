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
import grakn.core.logic.transformer.VariableTransformer;
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

            public DownstreamVars.Partial toDownstreamVars(Mapping mapping) {
                return new DownstreamVars.Partial(mapping.transform(conceptMap()), mapping);
            }

            public Optional<DownstreamVars.Partial> toDownstreamVars(Unifier unifier) {
                return unifier.unify(conceptMap()).map(unified -> new DownstreamVars.Partial(unified, unifier));
            }

        }

        public static class Derived extends Aggregated {

            Derived(ConceptMap partial, ConceptMap derivedFrom) {
                super(partial, derivedFrom);
            }

            public ConceptMap from() {
                return super.derivedFrom;
            }

            public ConceptMap map() {
                return new ConceptMap(conceptMap().concepts());
            }

            @Override
            public UpstreamVars.Derived asDerived() {
                return this;
            }
        }
    }

    public static class DownstreamVars {

        public static class Partial extends AnswerState {
            private final VariableTransformer transformer;

            Partial(ConceptMap conceptMap, @Nullable VariableTransformer transformer) {
                super(conceptMap);
                this.transformer = transformer;
            }

            public static Partial root() {
                // This is the entry-point answer state for the request received by the root resolver
                return new Partial(new ConceptMap(), null);
            }

            public Aggregated aggregateWith(ConceptMap conceptMap) {
                if (conceptMap == null) return null;
                if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                Map<Reference.Name, Concept> aggregatedMap = new HashMap<>(this.conceptMap().concepts());
                aggregatedMap.putAll(conceptMap.concepts());
                ConceptMap aggregated = new ConceptMap(aggregatedMap);
                return Aggregated.of(aggregated, this);
            }

            public UpstreamVars.Initial asUpstream() {
                return new UpstreamVars.Initial(map());
            }

            public VariableTransformer transformer() {
                return transformer;
            }

            public ConceptMap map() {
                return new ConceptMap(conceptMap().concepts());
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                if (!super.equals(o)) return false;
                final Partial partial = (Partial) o;
                return Objects.equals(transformer, partial.transformer);
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), transformer);
            }
        }

        public static class Mapped extends Aggregated {

            private final Mapping transformer;

            Mapped(ConceptMap aggregated, ConceptMap conceptMap, Mapping transformer) {
                super(aggregated, conceptMap);
                this.transformer = transformer;
            }

            public UpstreamVars.Derived toUpstreamVars() {
                return new UpstreamVars.Derived(transformer.unTransform(conceptMap()), conceptMap());
            }

            @Override
            public Mapped asMapped() {
                return this;
            }
        }

        public static class Unified extends Aggregated {

            private final Unifier transformer;

            Unified(ConceptMap aggregated, ConceptMap conceptMap, Unifier transformer) {
                super(aggregated, conceptMap);
                this.transformer = transformer;
            }

            public Optional<UpstreamVars.Derived> toUpstreamVars() {
                return transformer.unUnify(conceptMap()).map(t -> new UpstreamVars.Derived(t, conceptMap()));
            }

            @Override
            public Unified asUnified() {
                return this;
            }
        }
    }

    public abstract static class Aggregated extends AnswerState {

        private final ConceptMap derivedFrom;

        Aggregated(ConceptMap aggregated, ConceptMap derivedFrom) {
            super(aggregated);
            this.derivedFrom = derivedFrom;
        }

        public static Aggregated of(ConceptMap aggregated, DownstreamVars.Partial derivedFrom) {
            if (derivedFrom.transformer() == null) return new UpstreamVars.Derived(aggregated, derivedFrom.map());
            else if (derivedFrom.transformer().isUnifier())
                return new DownstreamVars.Unified(aggregated, derivedFrom.map(), derivedFrom.transformer.asUnifier());
            else if (derivedFrom.transformer().isMapping())
                return new DownstreamVars.Mapped(aggregated, derivedFrom.map(), derivedFrom.transformer.asMapped());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        public AnswerState.DownstreamVars.Mapped asMapped() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AnswerState.DownstreamVars.Mapped.class));
        }

        public AnswerState.DownstreamVars.Unified asUnified() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AnswerState.DownstreamVars.Unified.class));
        }

        public AnswerState.UpstreamVars.Derived asDerived() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AnswerState.UpstreamVars.Derived.class));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            final Aggregated that = (Aggregated) o;
            return derivedFrom.equals(that.derivedFrom);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), derivedFrom);
        }
    }
}
