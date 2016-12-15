package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.concept.Concept;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryAnswerStream {
    private final Stream<Map<String, Concept>> stream;

    public QueryAnswerStream(){ this.stream = Stream.empty();}
    public QueryAnswerStream(Stream<Map<String, Concept>> s){
        this.stream = s;
    }
    public Stream<Map<String, Concept>> stream(){ return stream;}

    public QueryAnswerStream filterVars(Set<String> vars) {
        return new QueryAnswerStream(
                this.stream.map(answer -> {
                    Map<String, Concept> filteredAnswer = new HashMap<>();
                    vars.stream()
                            .filter(answer::containsKey)
                            .forEach(var -> filteredAnswer.put(var, answer.get(var)));
                    return filteredAnswer;
                    })
                .filter(answer -> !answer.isEmpty())
        );
    }

    public QueryAnswerStream filterKnown(QueryAnswers known){
        return new QueryAnswerStream(
            this.stream.filter(answer ->{
                boolean isKnown = false;
                Iterator<Map<String, Concept>> it = known.iterator();
                while(it.hasNext() && !isKnown) {
                    Map<String, Concept> knownAnswer = it.next();
                    isKnown = knownAnswer.entrySet().containsAll(answer.entrySet());
                }
                return !isKnown;
            })
        );
    }

    public QueryAnswerStream filterIncomplete(Set<String> vars) {
        return new QueryAnswerStream(
                this.stream
                .filter(answer -> answer.size() == vars.size())
        );
    }

    public QueryAnswerStream filterNonEquals(Query query){
        Set<NotEquals> filters = query.getAtoms().stream()
                .filter(at -> at.getClass() == NotEquals.class)
                .map(at -> (NotEquals) at)
                .collect(Collectors.toSet());
        if(filters.isEmpty())
            return new QueryAnswerStream(this.stream);
        QueryAnswerStream results = new QueryAnswerStream();
        for (NotEquals filter : filters) results = filter.filter(this);
        return results;
    }

    private static Map<String, Concept> joinOperator(Map<String, Concept> m1, Map<String, Concept> m2){
        boolean isCompatible = true;
        Set<String> keysToCompare = new HashSet<>(m1.keySet());
        keysToCompare.retainAll(m2.keySet());
        Iterator<String> it = keysToCompare.iterator();
        while(it.hasNext() && isCompatible) {
            String var = it.next();
            isCompatible = m1.get(var).equals(m2.get(var));
        }
        if (isCompatible) {
            Map<String, Concept> merged = new HashMap<>(m1);
            merged.putAll(m2);
            return merged;
        } else return new HashMap<>();
    }

    public static BiFunction<Map<String, Concept>, Map<String, Concept>, Stream<Map<String, Concept>>> joinFunction = (a1, a2) -> {
        Map<String, Concept> merged = joinOperator(a1, a2);
        return merged.isEmpty()? Stream.empty(): Stream.of(merged);
    };

    public QueryAnswerStream join(QueryAnswerStream stream2) {
        Stream<Map<String, Concept>> result =  this.stream;
        Collection<Map<String, Concept>> c = stream2.stream().collect(Collectors.toSet());
        result = result.flatMap(a1 -> c.stream().flatMap(a2 -> joinFunction.apply(a1, a2)));
        return new QueryAnswerStream(result);
    }
}


