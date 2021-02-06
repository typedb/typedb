package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.exception.GraknException;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.resolvable.Negated;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.UpstreamVars.Initial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class NegationResolver extends Resolver<NegationResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(NegationResolver.class);

    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final Negated negated;
    private final Map<Request, NegationStatus> statuses;
    private boolean isInitialised;
    private Actor<? extends Resolver<?>> downstream;

    public NegationResolver(Actor<NegationResolver> self, Negated negated, ResolverRegistry registry,
                            TraversalEngine traversalEngine, Actor<ResolutionRecorder> resolutionRecorder,
                            boolean explanations) {
        super(self, NegationResolver.class.getSimpleName() + "(pattern: " + negated.pattern() + ")",
              registry, traversalEngine, explanations);
        this.negated = negated;
        this.resolutionRecorder = resolutionRecorder;
        this.statuses = new HashMap<>();
        this.isInitialised = false;
    }

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());

        List<Conjunction> disjunction = negated.pattern().conjunctions();
        if (disjunction.size() == 1) {
            downstream = registry.conjunction(disjunction.get(0));
        } else {
            // TODO
//            downstream = registry.disjunction(disjunction);
        }
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);

        if (!isInitialised) {
            initialiseDownstreamActors();
            isInitialised = true;
        }

        NegationStatus negationStatus = getOrInitialise(fromUpstream, iteration);
        if (negationStatus.status.isEmpty()) {
            assert negationStatus.externalIteration == iteration;
            tryAnswer(fromUpstream, negationStatus);
        } else if (negationStatus.status.isRequested()) {
            assert negationStatus.externalIteration == iteration;
            negationStatus.requested++;
            // we only ever sent 1 request into the negation resolvers
        } else if (negationStatus.status.isSatisfied()) {
            // TODO this is KEY - negations are SINGLE USE per unique request... otherwise we can end up looping infinitely right now. Discuss!
            // see in other places we call RespondToUpstream!!
            respondToUpstream(new Response.Fail(fromUpstream), iteration);
        } else if (negationStatus.status.isFailed()) {
            respondToUpstream(new Response.Fail(fromUpstream), iteration);
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
    }

    private NegationStatus getOrInitialise(Request fromUpstream, int iteration) {
        if (!statuses.containsKey(fromUpstream)) {
            statuses.put(fromUpstream, new NegationStatus(iteration));
        }
        return statuses.get(fromUpstream);
    }

    private void tryAnswer(Request fromUpstream, NegationStatus negationStatus) {
        // TODO if we wanted to accelerate the searching of a negation counter example,
        // TODO we could send multiple requests into the sub system at once!

        /*
        NOTE:
           Correctness: concludables that get reused in the negated portion, would conflate recursion/reiteration state from
              the toplevel root with the negation iterations, which we cannot allow. So, we must use THIS resolver
              as a sort of new root! TODO should NegationResolvers also implement a kind of Root interface??
        */
        Request request = Request.create(new Request.Path(list(self(), downstream)),
                                         Initial.of(fromUpstream.partialAnswer().conceptMap()).toDownstreamVars(),
                                         ResolutionAnswer.Derivation.EMPTY);
        requestFromDownstream(request, fromUpstream, 0);
        negationStatus.setRequested();
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}, therefore is FAILED", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        NegationStatus negationStatus = this.statuses.get(fromUpstream);

        negationStatus.setFailed();
        for (int i = 0; i < negationStatus.requested; i++) {
            respondToUpstream(new Response.Fail(fromUpstream), negationStatus.externalIteration);
        }
        negationStatus.requested = 0;
    }

    @Override
    protected void receiveExhausted(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Exhausted: {}, therefore is SATISFIED", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        NegationStatus negationStatus = this.statuses.get(fromUpstream);

        if (negationStatus.status.isRequested()) {
            negationStatus.setSatisfied();
            respondToUpstream(Response.Answer.create(fromUpstream, upstreamAnswer(fromUpstream)), negationStatus.externalIteration);
            negationStatus.requested--;
        }
        for (int i = 0; i < negationStatus.requested; i++) {
            // TODO this is KEY - negations are SINGLE USE per unique request... otherwise we can end up looping infinitely right now. Discuss!
            respondToUpstream(new Response.Fail(fromUpstream), negationStatus.externalIteration);
        }
        negationStatus.requested = 0;
    }

    private ResolutionAnswer upstreamAnswer(Request fromUpstream) {
        // TODO decide if we want to use isMapped here? Can Mapped currently act as a filter?
        assert fromUpstream.partialAnswer().isMapped();
        // TODO this is cheating, and incorrect!! We should be unmapping an empty map, but currently require to be equal size
        AnswerState.UpstreamVars.Derived derived = fromUpstream.partialAnswer().asMapped().mapToUpstream(fromUpstream.partialAnswer().conceptMap());

        ResolutionAnswer.Derivation derivation;
        if (explanations()) {
            // TODO
            derivation = null;
        } else {
            derivation = null;
        }

        // TODO double check that we just set isInferred to false
        return new ResolutionAnswer(derived, negated.pattern().toString(), derivation, self(), false);
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducer, int newIteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected void exception(Throwable e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private static class NegationStatus {

        final int externalIteration;
        int requested;
        Status status;

        public NegationStatus(int externalIteration) {
            this.externalIteration = externalIteration;
            this.requested = 0;
            this.status = Status.EMPTY;
        }

        public void setRequested() { this.status = Status.REQUESTED; }

        public void setFailed() { this.status = Status.FAILED; }

        public void setSatisfied() { this.status = Status.SATISFIED; }

        enum Status {
            EMPTY,
            REQUESTED,
            FAILED,
            SATISFIED;

            public boolean isEmpty() { return this == EMPTY; }

            public boolean isRequested() { return this == REQUESTED; }

            public boolean isFailed() { return this == FAILED; }

            public boolean isSatisfied() { return this == SATISFIED; }
        }
    }
}
