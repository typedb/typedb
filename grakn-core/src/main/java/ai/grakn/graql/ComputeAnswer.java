package ai.grakn.graql;

import ai.grakn.concept.Concept;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ComputeAnswer{

    Optional<Number> getNumber();

    ComputeAnswer setNumber(Number count);

    Optional<List<List<Concept>>> getPaths();

    ComputeAnswer setPaths(List<List<Concept>> paths);

    Optional<Map<Long, Set<String>>> getCountMap();

    ComputeAnswer setCountMap(Map<Long, Set<String>> countMap);
}
