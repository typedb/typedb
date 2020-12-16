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
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class AnswerState {
    // TODO Add equals and hashcode methods throughout
    private final ConceptMap conceptMap;

    AnswerState(ConceptMap conceptMap) {
        this.conceptMap = conceptMap;
    }
    protected ConceptMap conceptMap() {
        return conceptMap;
    }

    public static class UpstreamVars {

        public static class Partial extends AnswerState {

            Partial(ConceptMap conceptMap) {
                super(conceptMap);
            }

            public static UpstreamVars.Partial of(ConceptMap conceptMap) {
                return new UpstreamVars.Partial(conceptMap);
            }

            public DownstreamVars.Partial toDownstreamVars(Mapping mapping) {
                return new DownstreamVars.Partial(mapping.transform(conceptMap()), mapping);
            }

            public Optional<DownstreamVars.Partial> toDownstreamVars(Unifier unifier) {
                return unifier.transform(conceptMap()).map(unified -> new DownstreamVars.Partial(unified, unifier));
            }
        }

        public static class Derived extends AnswerState implements ResponseAnswer {

            private final ConceptMap derivedFrom;

            Derived(ConceptMap partial, ConceptMap derivedFrom) {
                super(partial);
                this.derivedFrom = derivedFrom;
            }

            public ConceptMap from() {
                return derivedFrom;
            }

            public ConceptMap map() {
                return new ConceptMap(conceptMap().concepts());
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

            public static Partial root() {
                // This is the entry answer state for the request received by the root resolver
                return new Partial(new ConceptMap(), null); // TODO Should the transformer be Nullable?
            }

            public Aggregated aggregateWith(ConceptMap conceptMap) {
                if (conceptMap == null) return null;
                if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                Map<Reference.Name, Concept> aggregatedMap = new HashMap<>(conceptMap().concepts());
                aggregatedMap.putAll(conceptMap.concepts());
                ConceptMap aggregated = new ConceptMap(aggregatedMap);
                return Aggregated.of(aggregated, this);
            }

            public UpstreamVars.Partial asUpstream() {
                return new UpstreamVars.Partial(map());
            }

            public VariableTransformer transformer() {
                return transformer;
            }

            public ConceptMap map() {
                return new ConceptMap(conceptMap().concepts());
            }
        }

        public abstract static class Aggregated extends AnswerState {

            private final ConceptMap derivedFrom;

            Aggregated(ConceptMap aggregated, ConceptMap derivedFrom) {
                super(aggregated);
                this.derivedFrom = derivedFrom;
            }

            public static Aggregated of(ConceptMap aggregated, Partial derivedFrom) {
                if (derivedFrom.transformer() == null) return new Root(aggregated, derivedFrom.map());
                else if (derivedFrom.transformer().isUnifier()) return new Unified(aggregated, derivedFrom.map(), derivedFrom.transformer.asUnifier());
                else if (derivedFrom.transformer().isMapping()) return new Mapped(aggregated, derivedFrom.map(), derivedFrom.transformer.asMapped());
                else throw GraknException.of(ILLEGAL_STATE);
            }

            public Mapped asMapped() {
                throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Mapped.class));
            }

            public Unified asUnified() {
                throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Unified.class));
            }

            public Root asRoot() {
                throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Root.class));
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
                    return transformer.unTransform(conceptMap()).map(t -> new UpstreamVars.Derived(t, conceptMap()));
                }

                @Override
                public Unified asUnified() {
                    return this;
                }
            }

            public static class Root extends Aggregated implements ResponseAnswer {
                // TODO Would like to make this class the same as Derived
                Root(ConceptMap aggregated, ConceptMap conceptMap) {
                    super(aggregated, conceptMap);
                }

                @Override
                public ConceptMap from() {
                    return super.derivedFrom;
                }

                @Override
                public ConceptMap map() {
                    return new ConceptMap(conceptMap().concepts());
                }

                @Override
                public Root asRoot() {
                    return this;
                }
            }
        }
    }
}
