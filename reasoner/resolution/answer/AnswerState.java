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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import graql.lang.pattern.variable.Reference;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class AnswerState {
    private final ConceptMap conceptMap;

    AnswerState(ConceptMap conceptMap) {
        this.conceptMap = conceptMap;
    }

    public static class UpstreamVars {

        public static class Partial extends AnswerState {

            Partial(ConceptMap conceptMap) {
                super(conceptMap);
            }

            public static UpstreamVars.Partial of(ConceptMap conceptMap) {
                return new UpstreamVars.Partial(conceptMap);
            }

            public DownstreamVars.Partial toDownstreamVars(VariableTransformer transformer) {
                return new DownstreamVars.Partial(transformer.transform(super.conceptMap), transformer);
            }
        }

        public static class Derived extends AnswerState {

            private final ConceptMap derivedFrom;

            Derived(ConceptMap partial, ConceptMap derivedFrom) {
                super(partial);
                this.derivedFrom = derivedFrom;
            }

            public ConceptMap from() {
                return derivedFrom;
            }

            public ConceptMap map() {
                return new ConceptMap(super.conceptMap.concepts());
            }
        }
    }

    public static class DownstreamVars {

        public static class Partial extends AnswerState {
            private final VariableTransformer transformer;

            Partial(ConceptMap conceptMap, VariableTransformer transformer) {
                super(conceptMap);
                this.transformer = transformer;
            }

            public Aggregated aggregateWith(ConceptMap conceptMap) {
                if (conceptMap == null) return null;
                if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                Map<Reference.Name, Concept> aggregatedMap = new HashMap<>(super.conceptMap.concepts());
                aggregatedMap.putAll(conceptMap.concepts());
                ConceptMap aggregatedConceptMap = new ConceptMap(aggregatedMap);
                return new Aggregated(aggregatedConceptMap, super.conceptMap, transformer);
            }

            public UpstreamVars.Partial asUpstream() {
                return new UpstreamVars.Partial(super.conceptMap);
            }

        }

        public abstract static class Aggregated extends AnswerState {

            private final ConceptMap derivedFrom;

            Aggregated(ConceptMap aggregated, ConceptMap derivedFrom) {
                super(aggregated);
                this.derivedFrom = derivedFrom;
            }

//            private final ConceptMap aggregated;
//            private final VariableTransformer transformer;

//            Aggregated(ConceptMap aggregated, ConceptMap conceptMap, VariableTransformer transformer) {
//                super(conceptMap);
//                this.aggregated = aggregated;
//                this.transformer = transformer;
//            }

//            public Optional<UpstreamVars.Derived> toUpstreamVars() {
//                return transformer.unTransform(this.aggregated).map(partial -> new UpstreamVars.Derived(partial, super.conceptMap));
//            }

            public Mapped asMapped() {}
            public Unified asUnified() {}

            public static class Mapped extends Aggregated {

                private final Mapping transformer;

                Mapped(ConceptMap aggregated, ConceptMap conceptMap, Mapping transformer) {
                    super(aggregated, conceptMap);
                    this.transformer = transformer;
                }

                public UpstreamVars.Derived toUpstreamVars() {
                    return new UpstreamVars.Derived(transformer.unTransform(super.conceptMap), super.conceptMap);
                }
            }

            public static class Unified extends Aggregated {

                Unified(ConceptMap aggregated, ConceptMap conceptMap, Unifier transformer) {
                    super(aggregated, conceptMap);
                }

                public UpstreamVars.Derived toUpstreamVars() {
                    return new UpstreamVars.Derived(super.transformer.unTransform(super.conceptMap).get(), super.conceptMap);
                }
            }

        }
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        final AnswerState that = (AnswerState) o;
//        return conceptMap.equals(that.conceptMap) &&
//                transformed.equals(that.transformed);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(conceptMap, transformed);
//    }

}
