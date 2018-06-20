package pick;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import java.util.stream.Stream;

// TODO I think I change this to be specific "CheckNotInRelationship" since being abstract gives complexity
public class NotInRelationshipConceptIdStream implements StreamInterface<ConceptId> {

    private String relationshipLabel;
    private String roleLabel;
    private StreamInterface<ConceptId> conceptIdStreamer;
    private Integer numAttemptsLimit;

    public NotInRelationshipConceptIdStream(String relationshipLabel,
                                            String roleLabel,
                                            Integer numAttemptsLimit,
                                            StreamInterface<ConceptId> conceptIdStreamer) {
        this.conceptIdStreamer = conceptIdStreamer;
        this.relationshipLabel = relationshipLabel;
        this.roleLabel = roleLabel;
        this.numAttemptsLimit = numAttemptsLimit;
    }

    @Override
    public Stream<ConceptId> getStream(int streamLength, GraknTx tx) {

        Stream<ConceptId> stream = this.conceptIdStreamer.getStream(streamLength, tx);

        QueryBuilder qb = tx.graql();

        // TODO Ideally this should also terminate when it's no longer possible to find the number of concepts required.
        // TODO Shouldn't the limit for the number of attempts be before the filter? Otherwise it will always wait
        return stream.limit(this.numAttemptsLimit * streamLength).filter(conceptId -> { //TODO Should probably be using stream.filter instead
                // Keep looking for a concept not in this kind of relationship, stop when one is found (by returning)
                // or the max number of attempts to find one has been reached

                Var x = Graql.var("x");
                Var r = Graql.var("r");
                // TODO To improve speed, here we could try to store a set of those conceptIds tried in the past and
                // found to be in a relationship, so that we don't have to check again. This amounts to doing some kind of storage
                boolean inRelationship = qb.match(x.id(conceptId), r.rel(this.roleLabel, x).isa(this.relationshipLabel)).get().execute().size() != 0;
                return !inRelationship;
        });
    }
}
