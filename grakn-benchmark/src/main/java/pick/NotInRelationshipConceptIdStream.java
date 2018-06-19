package pick;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;

import java.util.Iterator;
import java.util.stream.Stream;

// TODO I think I change this to be specific "CheckNotInRelationship" since being abstract gives complexity
public class NotInRelationshipConceptIdStream implements ConceptIdStreamInterface {

    private String relationshipLabel;
    private String roleLabel;
    private ConceptIdStreamInterface conceptIdStreamer;
    private Integer numAttemptsLimit;

    public NotInRelationshipConceptIdStream(String relationshipLabel,
                                            String roleLabel,
                                            Integer numAttemptsLimit,
                                            ConceptIdStreamInterface conceptIdStreamer) {
        this.conceptIdStreamer = conceptIdStreamer;
        this.relationshipLabel = relationshipLabel;
        this.roleLabel = roleLabel;
        this.numAttemptsLimit = numAttemptsLimit;
    }

    @Override
    public Stream<ConceptId> getConceptIdStream(int numConceptIds, GraknTx tx) {

        Stream<ConceptId> stream = this.conceptIdStreamer.getConceptIdStream(numConceptIds, tx);

        //TODO For each item in the stream, check for whether the given match varpattern is true
        // Unfortunately the varpattern will have to use a known variable name, x, to indicate the variable
        // corresponding to the conceptId

        Iterator<ConceptId> iter = stream.iterator();

        QueryBuilder qb = tx.graql();

        // TODO Ideally this should also terminate when it's no longer possible to find the number of concepts required.
        Stream<ConceptId> filteredStream = Stream.generate(() -> { //TODO Should probably be using stream.filter instead
            int numAttemptsMade = 0;

            while (numAttemptsMade <= this.numAttemptsLimit) {
                // Keep looking for a concept not in this kind of relationship, stop when one is found (by returning)
                // or the max number of attempts to find one has been reached

                Var x = Graql.var("x");
                Var r = Graql.var("r");
                ConceptId conceptId = iter.next();
                // TODO TO improve speed, here we could try to store a set of those conceptIds tried in the past and
                // found to be in a relationship, so that we don't have to check again. This amounts to doing some kind of storage
                boolean inRelationship = qb.match(x.id(conceptId), r.rel(this.roleLabel, x).isa(this.relationshipLabel)).get().execute().size() != 0;

                if (!inRelationship) {
                    return conceptId;
                }

                numAttemptsMade++;
            }
            return null;  // TODO How to propagate the problem in this case?
        });
        return filteredStream;
    }

}
