package pick;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.*;
import ai.grakn.graql.admin.Answer;

import java.util.*;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.count;

public class ConceptIdPicker implements ConceptIdStreamInterface {
    private Random rand;

    private Pattern matchVarPattern;
    private Var matchVar;

    public ConceptIdPicker(Random rand, Pattern matchVarPattern, Var matchVar) {
        this.rand = rand;
        this.matchVarPattern = matchVarPattern;
        this.matchVar = matchVar;
    }

    @Override
    public Stream<ConceptId> getConceptIdStream(int numConceptIds, GraknTx tx) {

        int typeCount = getConceptCount(tx);

        // If there aren't enough concepts to fulfill the number requested, then return null
        if (typeCount < numConceptIds) return null;

        Stream<Integer> randomUniqueOffsetStream = this.generateUniqueRandomOffsetStream(typeCount);
        Iterator<Integer> randomUniqueOffsetIterator = randomUniqueOffsetStream.iterator();

        Stream<ConceptId> stream = Stream.generate(() -> {

            Integer randomOffset = randomUniqueOffsetIterator.next();
            QueryBuilder qb = tx.graql();

            // TODO The following gives exception: Exception in thread "main" java.lang.ClassCastException: ai.grakn.remote.RemoteGraknTx cannot be cast to ai.grakn.kb.internal.EmbeddedGraknTx
            // TODO Waiting on bug fix
//            Stream<Concept> resultStream = qb.match(this.matchVarPattern)
//                    .offset(randomOffset)
//                    .limit(1)
//                    .get(this.matchVar);

            List<Answer> result = qb.match(this.matchVarPattern)
                    .offset(randomOffset)
                    .limit(1)
                    .get()
                    .execute();

            // Because we use limit 1, there will only be 1 result
            // return the ConceptId of the single variable in the single result
            return result.get(0).get(this.matchVar).getId();
        });

        return stream;
    }

    protected Integer getConceptCount(GraknTx tx) {
        QueryBuilder qb = tx.graql();
        // TODO This isn't working, waiting on bug fix
        Long count = qb.match(this.matchVarPattern)
                .aggregate(count())
                .execute();
        return Math.toIntExact(count);
    }

    private Stream<Integer> generateUniqueRandomOffsetStream(int offsetBound) {

        HashSet<Object> previousRandomOffsets = new HashSet<>();

        Stream<Integer> stream = Stream.generate(() -> {

            boolean foundUnique = false;

            int nextChoice = 0;
            while (!foundUnique) {
                nextChoice = rand.nextInt(offsetBound);
                foundUnique = previousRandomOffsets.add(nextChoice);
            }
            return nextChoice;
        });

        return stream;
    }
}
