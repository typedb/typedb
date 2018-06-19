package generator;

import ai.grakn.graql.Query;

import java.util.stream.Stream;

public interface GeneratorInterface {

    Stream<Query> generate();
}
