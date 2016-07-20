package io.mindmaps.graql.internal.shell;

import io.mindmaps.core.implementation.DataType;
import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.MatchQuery;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Completer that uses the graph ontology to find autocomplete candidates
 */
public class TypeCompleter implements GraqlCompleter {

    private final QueryBuilder builder;
    private final Collection<String> types;

    /**
     * @param builder the query builder to use for finding types in the graph
     */
    public TypeCompleter(QueryBuilder builder) {
        this.builder = builder;
        types = getTypes();
    }

    @Override
    public Stream<String> getCandidates(String buffer) {
        return types.stream();
    }

    private Collection<String> getTypes() {
        MatchQuery query = builder.match(QueryBuilder.var("type").isa(DataType.ConceptMeta.TYPE.getId()));
        return query.stream().map(r -> r.get("type").getId()).collect(Collectors.toSet());
    }
}
