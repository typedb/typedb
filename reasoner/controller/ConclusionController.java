/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisable;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisation;
import com.vaticle.typedb.core.reasoner.answer.PartialExplanation;
import com.vaticle.typedb.core.reasoner.controller.ConclusionController.Request.ConditionRequest;
import com.vaticle.typedb.core.reasoner.controller.ConclusionController.Request.MaterialiserRequest;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock.Connector.AbstractRequest;
import com.vaticle.typedb.core.reasoner.reactive.Input;
import com.vaticle.typedb.core.reasoner.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.reactive.common.Operator;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.reactive.TransformationStream.fanIn;

public abstract class ConclusionController<
        OUTPUT,
        REACTIVE_BLOCK extends AbstractReactiveBlock<Either<ConceptMap, Materialisation>, OUTPUT, ?, REACTIVE_BLOCK>,
        CONTROLLER extends ConclusionController<OUTPUT, REACTIVE_BLOCK, CONTROLLER>
        > extends AbstractController<
        ConceptMap,
        Either<ConceptMap, Materialisation>,
        OUTPUT,
        ConclusionController.Request<?, ?>,
        REACTIVE_BLOCK,
        CONTROLLER
        > {

    protected final Rule.Conclusion conclusion;
    protected final Driver<MaterialisationController> materialisationController;
    protected Driver<ConditionController> conditionController;

    public ConclusionController(Driver<CONTROLLER> driver, Rule.Conclusion conclusion,
                                Driver<MaterialisationController> materialisationController, Context context) {
        super(driver, context, () -> ConclusionController.class.getSimpleName() + "(pattern: " + conclusion + ")");
        this.conclusion = conclusion;
        this.materialisationController = materialisationController;
    }

    @Override
    public void setUpUpstreamControllers() {
        conditionController = registry().registerCondition(conclusion.rule().condition());
    }

    @Override
    public void resolveController(Request<?, ?> req) {
        if (isTerminated()) return;
        if (req.isCondition()) {
            conditionController.execute(actor -> actor.resolveReactiveBlock(
                    new AbstractReactiveBlock.Connector<>(req.asCondition().inputId(), req.asCondition().bounds())));
        } else if (req.isMaterialiser()) {
            materialisationController.execute(actor -> actor.resolveReactiveBlock(
                    new AbstractReactiveBlock.Connector<>(req.asMaterialiser().inputId(), req.asMaterialiser().bounds())));
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    public static class Match extends ConclusionController<Map<Variable, Concept>, ReactiveBlock.Match, Match> {

        public Match(Driver<Match> driver, Rule.Conclusion conclusion,
                     Driver<MaterialisationController> materialisationController, Context context) {
            super(driver, conclusion, materialisationController, context);
        }

        @Override
        protected ReactiveBlock.Match createReactiveBlockFromDriver(Driver<ReactiveBlock.Match> reactiveBlockDriver,
                                                                    ConceptMap bounds) {
            return new ReactiveBlock.Match(
                    reactiveBlockDriver, driver(), reactiveBlockContext(), conclusion.rule(), bounds,
                    registry().conceptManager(),
                    () -> ReactiveBlock.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
            );
        }

    }

    public static class Explain extends ConclusionController<PartialExplanation, ReactiveBlock.Explain, Explain> {

        public Explain(Driver<Explain> driver, Rule.Conclusion conclusion,
                       Driver<MaterialisationController> materialisationController, Context context) {
            super(driver, conclusion, materialisationController, context);
        }

        @Override
        protected ReactiveBlock.Explain createReactiveBlockFromDriver(Driver<ReactiveBlock.Explain> reactiveBlockDriver,
                                                                      ConceptMap bounds) {
            return new ReactiveBlock.Explain(
                    reactiveBlockDriver, driver(), reactiveBlockContext(), conclusion.rule(), bounds,
                    registry().conceptManager(),
                    () -> ReactiveBlock.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
            );
        }
    }

    protected static class Request<CONTROLLER_ID, BOUNDS>
            extends AbstractRequest<CONTROLLER_ID, BOUNDS, Either<ConceptMap, Materialisation>> {

        protected Request(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputId,
                          CONTROLLER_ID controller_id, BOUNDS bounds) {
            super(inputId, controller_id, bounds);
        }

        public boolean isCondition() {
            return false;
        }

        public ConditionRequest asCondition() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isMaterialiser() {
            return false;
        }

        public MaterialiserRequest asMaterialiser() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        protected static class ConditionRequest extends Request<Rule.Condition, ConceptMap> {

            public ConditionRequest(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputId,
                                    Rule.Condition controllerId, ConceptMap reactiveBlockId) {
                super(inputId, controllerId, reactiveBlockId);
            }

            @Override
            public boolean isCondition() {
                return true;
            }

            @Override
            public ConditionRequest asCondition() {
                return this;
            }

        }

        protected static class MaterialiserRequest extends Request<Void, Materialisable> {

            public MaterialiserRequest(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputId,
                                       Void controllerId, Materialisable reactiveBlockId) {
                super(inputId, controllerId, reactiveBlockId);
            }

            @Override
            public boolean isMaterialiser() {
                return true;
            }

            @Override
            public MaterialiserRequest asMaterialiser() {
                return this;
            }

        }
    }

    protected abstract static class ReactiveBlock<OUTPUT, REACTIVE_BLOCK extends ReactiveBlock<OUTPUT, REACTIVE_BLOCK>>
            extends AbstractReactiveBlock<
            Either<ConceptMap, Materialisation>, OUTPUT,
            Request<?, ?>, REACTIVE_BLOCK
            > {

        private final Rule rule;
        private final ConceptMap bounds;
        private final ConceptManager conceptManager;
        private final Set<ConditionRequest> conditionRequests;
        private final Set<MaterialiserRequest> materialisationRequests;

        protected ReactiveBlock(Driver<REACTIVE_BLOCK> driver,
                                Driver<? extends ConclusionController<OUTPUT, REACTIVE_BLOCK, ?>> controller,
                                Context context, Rule rule, ConceptMap bounds, ConceptManager conceptManager,
                                Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.rule = rule;
            this.bounds = bounds;
            this.conceptManager = conceptManager;
            this.conditionRequests = new HashSet<>();
            this.materialisationRequests = new HashSet<>();
        }

        protected Rule rule() {
            return rule;
        }

        @Override
        public void setUp() {
            setOutputRouter(PoolingStream.fanOut(this));
            Input<Either<ConceptMap, Materialisation>> conditionInput = createInput();
            ConceptMap filteredBounds = bounds.filter(rule.condition().conjunction().retrieves());
            mayRequestCondition(new ConditionRequest(conditionInput.identifier(), rule.condition(), filteredBounds));
            Stream<Either<ConceptMap, Map<Variable, Concept>>, OUTPUT> conclusionReactive = fanIn(this, createOperator());
            conditionInput.map(ReactiveBlock::convertConclusionInput).registerSubscriber(conclusionReactive);
            conclusionReactive.registerSubscriber(outputRouter());
        }

        protected abstract ConclusionOperator<OUTPUT> createOperator();

        private static Either<ConceptMap, Map<Variable, Concept>> convertConclusionInput(Either<ConceptMap, Materialisation> input) {
            return Either.first(input.first());
        }

        private void mayRequestCondition(ConditionRequest conditionRequest) {
            if (!conditionRequests.contains(conditionRequest)) {
                conditionRequests.add(conditionRequest);
                requestConnection(conditionRequest);
            }
        }

        private void mayRequestMaterialiser(MaterialiserRequest materialisationRequest) {
            // TODO: Does this method achieve anything?
            if (!materialisationRequests.contains(materialisationRequest)) {
                materialisationRequests.add(materialisationRequest);
                requestConnection(materialisationRequest);
            }
        }

        public static class Match extends ReactiveBlock<Map<Variable, Concept>, Match> {

            protected Match(Driver<Match> driver, Driver<ConclusionController.Match> controller, Context context,
                            Rule rule, ConceptMap bounds, ConceptManager conceptManager, Supplier<String> debugName) {
                super(driver, controller, context, rule, bounds, conceptManager, debugName);
            }

            @Override
            protected ConclusionOperator<Map<Variable, Concept>> createOperator() {
                return new ConclusionOperator.Match(this);
            }
        }

        public static class Explain extends ReactiveBlock<PartialExplanation, Explain> {

            protected Explain(Driver<Explain> driver,
                              Driver<? extends ConclusionController<PartialExplanation, Explain, ?>> controller,
                              Context context, Rule rule, ConceptMap bounds, ConceptManager conceptManager,
                              Supplier<String> debugName) {
                super(driver, controller, context, rule, bounds, conceptManager, debugName);
            }

            @Override
            protected ConclusionOperator<PartialExplanation> createOperator() {
                return new ConclusionOperator.Explain(this);
            }

        }

        private static abstract class ConclusionOperator<OUTPUT> implements Operator.Transformer<Either<ConceptMap, Map<Variable, Concept>>, OUTPUT> {

            private final ReactiveBlock<?, ?> reactiveBlock;

            private ConclusionOperator(ReactiveBlock<?, ?> reactiveBlock) {
                this.reactiveBlock = reactiveBlock;
            }

            protected ReactiveBlock<?, ?> reactiveBlock() {
                return reactiveBlock;
            }

            @Override
            public Set<Publisher<Either<ConceptMap, Map<Variable, Concept>>>> initialNewPublishers() {
                return set();  // TODO: This could create the connection to the condition
            }

            @Override
            public Either<Publisher<Either<ConceptMap, Map<Variable, Concept>>>, Set<OUTPUT>> accept(
                    Publisher<Either<ConceptMap, Map<Variable, Concept>>> publisher,
                    Either<ConceptMap, Map<Variable, Concept>> packet
            ) {
                if (packet.isFirst()) {
                    assert packet.first().concepts().keySet().containsAll(reactiveBlock().rule().condition().conjunction().retrieves());
                    Input<Either<ConceptMap, Materialisation>> materialisationInput = reactiveBlock().createInput();
                    ConceptMap filteredConditionAns = packet.first().filter(reactiveBlock().rule().conclusion().retrievableIds());  // TODO: no explainables carried forwards
                    reactiveBlock().mayRequestMaterialiser(new MaterialiserRequest(
                            materialisationInput.identifier(), null,
                            reactiveBlock().rule().conclusion().materialisable(filteredConditionAns, reactiveBlock().conceptManager))
                    );
                    Publisher<Either<ConceptMap, Map<Variable, Concept>>> op = materialisationInput
                            .map(m -> m.second().bindToConclusion(reactiveBlock().rule().conclusion(), filteredConditionAns))
                            .flatMap(m -> merge(m, reactiveBlock().bounds))
                            .map(Either::second);
                    mayStoreConditionAnswer(op, packet.first());
                    return Either.first(op);
                } else {
                    return Either.second(set(packageAnswer(publisher, packet.second())));
                }
            }

            private static FunctionalIterator<Map<Variable, Concept>> merge(Map<Variable, Concept> materialisation, ConceptMap bounds) {
                for (Map.Entry<Variable, Concept> entry : materialisation.entrySet()) {
                    Variable v = entry.getKey();
                    Concept c = entry.getValue();
                    if (v.isRetrievable() && bounds.contains(v.asRetrievable())
                            && !bounds.get(v.asRetrievable()).equals(c)) {
                        return Iterators.empty();
                    }
                }
                return Iterators.single(materialisation);
            }

            protected abstract void mayStoreConditionAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialisationInput, ConceptMap conditionAnswer);

            protected abstract OUTPUT packageAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> publisher, Map<Variable, Concept> conclusionAnswer);

            public static class Match extends ConclusionOperator<Map<Variable, Concept>> {

                private Match(ReactiveBlock<?, ?> reactiveBlock) {
                    super(reactiveBlock);
                }

                @Override
                protected void mayStoreConditionAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialisationInput, ConceptMap conditionAnswer) {}

                @Override
                protected Map<Variable, Concept> packageAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> publisher, Map<Variable, Concept> conclusionAnswer) {
                    return conclusionAnswer;
                }
            }

            public static class Explain extends ConclusionOperator<PartialExplanation> {

                private final Map<Publisher<Either<ConceptMap, Map<Variable, Concept>>>, ConceptMap> conditionAnswers;

                private Explain(ReactiveBlock<?, ?> reactiveBlock) {
                    super(reactiveBlock);
                    this.conditionAnswers = new HashMap<>();
                }

                @Override
                protected void mayStoreConditionAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialisationInput, ConceptMap conditionAnswer) {
                    conditionAnswers.put(materialisationInput, conditionAnswer);
                }

                @Override
                protected PartialExplanation packageAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialiserInput, Map<Variable, Concept> conclusionAnswer) {
                    return new PartialExplanation(reactiveBlock().rule(), conclusionAnswer, conditionAnswers.get(materialiserInput));
                }
            }

        }

    }

}
