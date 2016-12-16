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

    public QueryAnswerStream(Stream<Map<String, Concept>> s) {
        this.stream = s;
    }
    public Stream<Map<String, Concept>> stream() {return stream;}

    private static Map<String, Concept> varFilterOperator(Map<String, Concept> answer, Set<String> vars) {
        Map<String, Concept> filteredAnswer = new HashMap<>();
        vars.stream()
                .filter(answer::containsKey)
                .forEach(var -> filteredAnswer.put(var, answer.get(var)));
        return filteredAnswer;
    }

    private static boolean knownFilterOperator(Map<String, Concept> answer, QueryAnswers known) {
        boolean isKnown = false;
        Iterator<Map<String, Concept>> it = known.iterator();
        while (it.hasNext() && !isKnown) {
            Map<String, Concept> knownAnswer = it.next();
            isKnown = knownAnswer.entrySet().containsAll(answer.entrySet());
        }
        return !isKnown;
    }

    private static boolean nonEqualsOperator(Map<String, Concept> answer, Set<NotEquals> atoms) {
        if(atoms.isEmpty()) return false;
        boolean filter = false;
        Iterator<NotEquals> it = atoms.iterator();
        while (it.hasNext() && !filter)
            filter = NotEquals.notEqualsOperator(answer, it.next());
        return filter;
    }

    public static final BiFunction<Map<String, Concept>, Set<String>, Stream<Map<String, Concept>>> varFilterFunction = (a, vars) -> {
        Map<String, Concept> filteredAnswer = varFilterOperator(a, vars);
        return filteredAnswer.isEmpty() ? Stream.empty() : Stream.of(filteredAnswer);
    };

    public static final BiFunction<Map<String, Concept>, QueryAnswers, Stream<Map<String, Concept>>> knownFilterFunction =
            (a, known) -> knownFilterOperator(a, known) ? Stream.empty() : Stream.of(a);

    public static final BiFunction<Map<String, Concept>, Set<String>, Stream<Map<String, Concept>>> incompleteFilterFunction =
            (a, vars) -> a.size() == vars.size() ? Stream.of(a) : Stream.empty();

    public static final BiFunction<Map<String, Concept>, Set<NotEquals>, Stream<Map<String, Concept>>> nonEqualsFilterFunction =
            (a, atoms) -> nonEqualsOperator(a, atoms) ? Stream.empty() : Stream.of(a);

    public QueryAnswerStream filterVars(Set<String> vars) {
        return new QueryAnswerStream(this.stream.flatMap(a -> varFilterFunction.apply(a, vars)));
    }

    public QueryAnswerStream filterKnown(QueryAnswers known){
        return new QueryAnswerStream(this.stream.flatMap(a -> knownFilterFunction.apply(a, known)));
    }

    public QueryAnswerStream filterIncomplete(Set<String> vars) {
        return new QueryAnswerStream(this.stream.flatMap(a -> incompleteFilterFunction.apply(a, vars)));
    }

    public QueryAnswerStream filterNonEquals(Query query){
        Set<NotEquals> filters = query.getFilters();
        if(filters.isEmpty()) return new QueryAnswerStream(this.stream);
        return new QueryAnswerStream(this.stream.flatMap(a -> nonEqualsFilterFunction.apply(a, filters)));
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

    private static final BiFunction<Map<String, Concept>, Map<String, Concept>, Stream<Map<String, Concept>>> joinFunction = (a1, a2) -> {
        Map<String, Concept> merged = joinOperator(a1, a2);
        return merged.isEmpty()? Stream.empty(): Stream.of(merged);
    };

    public QueryAnswerStream join(QueryAnswerStream stream2) {
        Stream<Map<String, Concept>> result =  this.stream;
        Collection<Map<String, Concept>> c = stream2.stream().collect(Collectors.toSet());
        result = result.flatMap(a1 -> c.stream().flatMap(a2 -> joinFunction.apply(a1, a2)));
        return new QueryAnswerStream(result);
    }

    public static Stream<Map<String, Concept>> join(Stream<Map<String, Concept>> stream, Stream<Map<String, Concept>> stream2) {
        Collection<Map<String, Concept>> c = stream2.collect(Collectors.toSet());
        return stream.flatMap(a1 -> c.stream().flatMap(a2 -> joinFunction.apply(a1, a2)));
    }
}


