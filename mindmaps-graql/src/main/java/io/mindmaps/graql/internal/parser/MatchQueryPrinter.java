package io.mindmaps.graql.internal.parser;

import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.api.query.MatchQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A printer for a MatchQuery, that uses a map of Getters to decide what information to print
 */
public class MatchQueryPrinter {

    private final MatchQuery matchQuery;
    private final Map<String, List<Getter>> getters = new HashMap<>();

    /**
     * @param matchQuery the match query whose results should be printed
     */
    public MatchQueryPrinter(MatchQuery matchQuery) {
        this.matchQuery = matchQuery;
    }

    /**
     * @return the MatchQuery that this printer will print
     */
    public MatchQuery getMatchQuery() {
        return matchQuery;
    }

    /**
     * @param name the variable name to apply the getter to
     * @param getter the getter to apply to the results of the query
     */
    public void addGetter(String name, Getter getter) {
        getters.computeIfAbsent(name, k -> new ArrayList<>()).add(getter);
    }

    /**
     * @return a stream of strings, where each string represents a result
     */
    public Stream<String> resultsString() {
        return matchQuery.stream().map(results -> {
            StringBuilder str = new StringBuilder();

            results.forEach((name, concept) -> {
                List<Getter> getterList = getGetters(name);
                Concept result = results.get(name);
                str.append("$").append(name);
                getterList.forEach(getter -> str.append(getter.resultString(result)));
                str.append("; ");
            });

            return str.toString();
        });
    }

    private List<Getter> getGetters(String name) {
        List<Getter> getterList = this.getters.computeIfAbsent(name, k -> new ArrayList<>());

        // Add default getters if none provided
        if (getterList.isEmpty()) {
            getterList.add(Getter.id());
            getterList.add(Getter.value());
            getterList.add(Getter.isa());
        }

        return getterList;
    }
}
