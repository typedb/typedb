package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class RuleResolvers {

    public static class Condition extends ConjunctionResolver<Condition> {

        private static final Logger LOG = LoggerFactory.getLogger(Condition.class);

        private final Rule rule;

        public Condition(Actor<Condition> self, Rule rule, Actor<ResolutionRecorder> resolutionRecorder,
                         ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                         LogicManager logicMgr, Planner planner, boolean explanations) {
            super(self, Condition.class.getCanonicalName() + "(rule:" + rule + ")", rule.when(), resolutionRecorder,
                  registry, traversalEngine, conceptMgr, logicMgr, planner, explanations);
            this.rule = rule;
        }

        @Override
        protected void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
            if (responseProducer.hasUpstreamAnswer()) {
                AnswerState.Partial<?> upstreamAnswer = responseProducer.upstreamAnswers().next();
                responseProducer.recordProduced(upstreamAnswer.conceptMap());
                answerToUpstream(upstreamAnswer, fromUpstream, iteration);
            } else {
                if (responseProducer.hasDownstreamProducer()) {
                    requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
                } else {
                    failToUpstream(fromUpstream, iteration);
                }
            }
        }

        @Override
        protected Optional<AnswerState> toUpstreamAnswer(AnswerState.Partial<?> fromDownstream) {
            return Optional.of(fromDownstream.asFiltered().toUpstream(self()));
        }

        @Override
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the resolvers and report and exception to root
        }
    }

    public static class Conclusion extends Resolver<Conclusion> {

        private static final Logger LOG = LoggerFactory.getLogger(Conclusion.class);

        private final Actor<ResolutionRecorder> resolutionRecorder;
        private final Rule.Conclusion conclusion;
        private final Map<Request, ConclusionResponses> conclusionResponsess;
        private boolean isInitialised;
        private Actor<Condition> ruleResolver;

        public Conclusion(Actor<Conclusion> self, Rule.Conclusion conclusion, ResolverRegistry registry,
                                  Actor<ResolutionRecorder> resolutionRecorder, TraversalEngine traversalEngine,
                                  ConceptManager conceptMgr, boolean explanations) {
            super(self, Conclusion.class.getSimpleName() + "(rule.conclusion: " + conclusion + ")",
                  registry, traversalEngine, conceptMgr, explanations);
            this.conclusion = conclusion;
            this.resolutionRecorder = resolutionRecorder;
            this.conclusionResponsess = new HashMap<>();
            this.isInitialised = false;
        }

        @Override
        public void receiveRequest(Request fromUpstream, int iteration) {
            LOG.trace("{}: received Request: {}", name(), fromUpstream);

            if (!isInitialised) initialiseDownstreamResolvers();

            ConclusionResponses conclusionResponses = getOrUpdateResponses(fromUpstream, iteration);

            if (iteration < conclusionResponses.iteration()) {
                // short circuit if the request came from a prior iteration
                failToUpstream(fromUpstream, iteration);
            } else {
                assert iteration == conclusionResponses.iteration();
                tryAnswer(fromUpstream, conclusionResponses, iteration);
            }
        }

        private void tryAnswer(Request fromUpstream, ConclusionResponses conclusionResponses, int iteration) {
            // TODO try to use a downstream that is available, or a local iterator
            // TODO to produce an upstream answer

            Optional<AnswerState.Partial<?>> answer = conclusionResponses.nextResponse();
            if (answer.isPresent()) {
                conclusionResponses.recordProduced(answer.get().conceptMap());
                answerToUpstream(answer.get(), fromUpstream, iteration);
            } else if (conclusionResponses.hasDownstream()) {
                requestFromDownstream(conclusionResponses.nextDownstream(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }

        @Override
        protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
            LOG.trace("{}: received Answer: {}", name(), fromDownstream);

            Request toDownstream = fromDownstream.sourceRequest();
            Request fromUpstream = fromUpstream(toDownstream);
            ConclusionResponses conclusionResponses = conclusionResponsess.get(fromUpstream);

            ResourceIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                    .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);
            if (!materialisations.hasNext()) throw GraknException.of(ILLEGAL_STATE);

            ResourceIterator<AnswerState.Partial<?>> materialisedAnswers = materialisations
                    .map(concepts -> fromUpstream.partialAnswer().asUnified().aggregateToUpstream(concepts, self()))
                    .filter(Optional::isPresent)
                    .map(Optional::get);
            conclusionResponses.addResponses(materialisedAnswers);
            tryAnswer(fromUpstream, conclusionResponses, iteration);
        }

        @Override
        protected void receiveFail(Response.Fail fromDownstream, int iteration) {
            LOG.trace("{}: received Fail: {}", name(), fromDownstream);
            Request toDownstream = fromDownstream.sourceRequest();
            Request fromUpstream = fromUpstream(toDownstream);
            ConclusionResponses conclusionResponses = conclusionResponsess.get(fromUpstream);

            if (iteration < conclusionResponses.iteration()) {
                // short circuit old iteration fail messages to upstream
                failToUpstream(fromUpstream, iteration);
                return;
            }

            conclusionResponses.removeDownstream(fromDownstream.sourceRequest());
            tryAnswer(fromUpstream, conclusionResponses, iteration);
        }

        @Override
        protected void initialiseDownstreamResolvers() {
            LOG.debug("{}: initialising downstream resolvers", name());
            ruleResolver = registry.registerCondition(conclusion.rule());
            isInitialised = true;
        }

        private ConclusionResponses getOrUpdateResponses(Request fromUpstream, int iteration) {
            if (!conclusionResponsess.containsKey(fromUpstream)) {
                conclusionResponsess.put(fromUpstream, createConclusionResponses(fromUpstream, iteration));
            } else {
                ConclusionResponses conclusionResponses = conclusionResponsess.get(fromUpstream);
                assert conclusionResponses.iteration() == iteration ||
                        conclusionResponses.iteration() + 1 == iteration;

                if (conclusionResponses.iteration() + 1 == iteration) {
                    // when the same request for the next iteration the first time, re-initialise required state
                    ConclusionResponses conclusionResponsesNextIter = createConclusionResponses(fromUpstream, iteration);
                    conclusionResponsess.put(fromUpstream, conclusionResponsesNextIter);
                }
            }
            return conclusionResponsess.get(fromUpstream);
        }

        private ConclusionResponses createConclusionResponses(Request fromUpstream, int iteration) {
            LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);

            ConclusionResponses conclusionResponses = new ConclusionResponses(iteration);

            ConceptMap partialAnswer = fromUpstream.partialAnswer().conceptMap();
            // we do a extra traversal to expand the partial answer if we already have the concept that is meant to be generated
            // and if there's extra variables to be populated
            assert conclusion.retrievableIds().containsAll(partialAnswer.concepts().keySet());
            if (conclusion.generating().isPresent() && conclusion.retrievableIds().size() > partialAnswer.concepts().size() &&
                    partialAnswer.concepts().containsKey(conclusion.generating().get().id())) {
                ResourceIterator<AnswerState.Partial.Filtered> completedAnswers = candidateAnswers(fromUpstream, partialAnswer);
                completedAnswers.forEachRemaining(answer -> conclusionResponses.addDownstream(Request.create(self(), ruleResolver,
                                                                                                             answer)));
            } else {
                Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
                AnswerState.Partial.Filtered downstreamAnswer = fromUpstream.partialAnswer().filterToDownstream(named);
                conclusionResponses.addDownstream(Request.create(self(), ruleResolver, downstreamAnswer));
            }

            return conclusionResponses;
        }

        private ResourceIterator<AnswerState.Partial.Filtered> candidateAnswers(Request fromUpstream, ConceptMap answer) {
            ResourceIterator<ConceptMap> traversal = boundAnswers(conclusion.conjunction(), answer);
            Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
            return traversal.map(ans -> fromUpstream.partialAnswer().asUnified().extend(ans).filterToDownstream(named));
        }

        @Override
        protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
            throw GraknException.of(ILLEGAL_STATE);
        }

        @Override
        protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer conclusionResponses, int newIteration) {
            throw GraknException.of(ILLEGAL_STATE);
        }

        @Override
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the resolvers and report and exception to root
        }

        private static class ConclusionResponses {

            private final List<ResourceIterator<AnswerState.Partial<?>>> materialisedAnswers;
            private final LinkedHashSet<Request> downstreams;
            private final Set<ConceptMap> produced;
            private Iterator<Request> downstreamProducerSelector;
            private final int iteration;

            public ConclusionResponses(int iteration) {
                this.materialisedAnswers = new LinkedList<>();
                this.downstreams = new LinkedHashSet<>();
                this.produced = new HashSet<>();
                this.iteration = iteration;
                downstreamProducerSelector = downstreams.iterator();
            }

            public int iteration() {
                return iteration;
            }

            public boolean hasProduced(ConceptMap conceptMap) {
                return produced.contains(conceptMap);
            }

            public void recordProduced(ConceptMap conceptMap) {
                produced.add(conceptMap);
            }

            public void addResponses(ResourceIterator<AnswerState.Partial<?>> materialisations) {
                materialisedAnswers.add(materialisations.filter(partial -> !hasProduced(partial.conceptMap())));
            }

            public Optional<AnswerState.Partial<?>> nextResponse() {
                if (hasResponse()) return Optional.of(materialisedAnswers.get(0).next());
                else return Optional.empty();
            }

            private boolean hasResponse() {
                while (!materialisedAnswers.isEmpty() && !materialisedAnswers.get(0).hasNext()) {
                    materialisedAnswers.remove(0);
                }
                return !materialisedAnswers.isEmpty();
            }

            public boolean hasDownstream() {
                return !downstreams.isEmpty();
            }

            public Request nextDownstream() {
                if (!downstreamProducerSelector.hasNext()) downstreamProducerSelector = downstreams.iterator();
                return downstreamProducerSelector.next();
            }

            public void removeDownstream(Request request) {
                boolean removed = downstreams.remove(request);
                // only update the iterator when removing an element, to avoid resetting and reusing first request too often
                // note: this is a large performance win when processing large batches of requests
                if (removed) downstreamProducerSelector = downstreams.iterator();
            }

            public void addDownstream(Request request) {
                assert !(downstreams.contains(request)) : "downstream answer producer already contains this request";
                downstreams.add(request);
                downstreamProducerSelector = downstreams.iterator();
            }
        }
    }
}
