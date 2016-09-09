package io.mindmaps.graql.internal.reasoner.container;

import io.mindmaps.concept.Concept;

import java.util.*;
import java.util.stream.Collectors;

public class QueryAnswers extends HashSet<Map<String, Concept>> {

    public QueryAnswers(){super();}
    public QueryAnswers(Collection<? extends Map<String, Concept>> ans){ super(ans);}

    public QueryAnswers filter(Set<String> vars) {
        QueryAnswers results = new QueryAnswers();
        if (this.isEmpty()) return results;

        this.forEach(answer -> {
            Map<String, Concept> map = new HashMap<>();
            answer.forEach((var, concept) -> {
                if (vars.contains(var))
                    map.put(var, concept);
            });
            if (!map.isEmpty()) results.add(map);
        });

        return new QueryAnswers(results.stream().distinct().collect(Collectors.toSet()));
    }
}
