package io.mindmaps.graql.internal.query;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.DataType;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.query.*;
import io.mindmaps.graql.internal.AdminConverter;
import io.mindmaps.graql.internal.gremlin.Query;
import io.mindmaps.graql.internal.validation.ErrorMessage;
import io.mindmaps.graql.internal.validation.MatchQueryValidator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.mindmaps.core.implementation.DataType.ConceptPropertyUnique.ITEM_IDENTIFIER;
import static io.mindmaps.core.implementation.DataType.EdgeLabel.SHORTCUT;
import static io.mindmaps.core.implementation.DataType.EdgeProperty.TO_TYPE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of MatchQuery for finding patterns in graphs
 */
public class MatchQueryImpl implements MatchQuery.Admin {

    private final Set<String> names;
    private boolean userDefinedNames = false;
    private final Pattern.Conjunction<Pattern.Admin> pattern;

    private Optional<Long> limit = Optional.empty();
    private long offset = 0;
    private boolean distinct = false;

    private Optional<String> orderVar = Optional.empty();
    private Optional<String> orderResourceType = Optional.empty();
    private boolean orderAsc = true;

    private MindmapsTransaction transaction;

    /**
     * @param transaction a transaction to execute the match query on
     * @param pattern a pattern to match in the graph
     */
    public MatchQueryImpl(MindmapsTransaction transaction, Pattern.Conjunction<Pattern.Admin> pattern) {
        if (pattern.getPatterns().size() == 0) {
            throw new IllegalArgumentException(ErrorMessage.MATCH_NO_PATTERNS.getMessage());
        }

        this.transaction = transaction;
        this.pattern = pattern;

        // Select all user defined names
        this.names = this.pattern.getVars().stream()
                .flatMap(v -> v.getInnerVars().stream())
                .filter(Var.Admin::isUserDefinedName)
                .map(Var.Admin::getName)
                .collect(Collectors.toSet());

        if (transaction != null) validate();
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        List<GraphTraversal<Vertex, Map<String, Vertex>>> traversals = getQuery().getTraversals();

        traversals.forEach(this::applyModifiers);

        List<Stream<Map<String, Vertex>>> streams = traversals.parallelStream().map(Traversal::toStream).collect(toList());

        Stream<Map<String, Vertex>> stream;

        if (orderVar.isPresent()) {
            // Merge together all streams, maintaining any existing sort order
            stream = mergeSortedStreams(streams, resultComparator());
        } else {
            // Join together streams in no particular order
            stream = streams.parallelStream().unordered().flatMap(Function.identity());
        }

        if (distinct) stream = stream.distinct();
        stream = stream.skip(offset);
        if (limit.isPresent()) stream = stream.limit(limit.get());

        return stream.map(this::makeResults).sequential();
    }

    @Override
    public MatchQuery select(Collection<String> names) {
        if (names.isEmpty()) {
            throw new IllegalArgumentException(ErrorMessage.SELECT_NONE_SELECTED.getMessage());
        }

        userDefinedNames = true;

        names.forEach(
                name -> {
                    if (!this.names.contains(name)) {
                        throw new IllegalStateException(ErrorMessage.SELECT_VAR_NOT_IN_MATCH.getMessage(name));
                    }
                }
        );

        this.names.retainAll(names);
        return this;
    }

    @Override
    public Streamable<Concept> get(String name) {
        return () -> stream().map(result -> result.get(name));
    }

    @Override
    public AskQuery ask() {
        return new AskQueryImpl(this);
    }

    @Override
    public InsertQuery insert(Collection<? extends Var> vars) {
        return new InsertQueryImpl(transaction, AdminConverter.getVarAdmins(vars), this);
    }

    @Override
    public DeleteQuery delete(Collection<? extends Var> deleters) {
        return new DeleteQueryImpl(transaction, AdminConverter.getVarAdmins(deleters), this);
    }

    @Override
    public MatchQuery withTransaction(MindmapsTransaction transaction) {
        this.transaction = Objects.requireNonNull(transaction);
        validate();
        return this;
    }

    @Override
    public MatchQuery limit(long limit) {
        this.limit = Optional.of(limit);
        return this;
    }

    @Override
    public MatchQuery offset(long offset) {
        this.offset = offset;
        return this;
    }

    @Override
    public MatchQuery distinct() {
        distinct = true;
        return this;
    }

    @Override
    public MatchQuery orderBy(String varName, boolean asc) {
        orderVar = Optional.of(varName);
        orderAsc = asc;
        return this;
    }

    @Override
    public MatchQuery orderBy(String varName, String resourceType, boolean asc) {
        orderVar = Optional.of(varName);
        orderResourceType = Optional.of(resourceType);
        orderAsc = asc;
        return this;
    }

    @Override
    public Admin admin() {
        return this;
    }

    @Override
    public Set<Type> getTypes() {
        return getQuery().getConcepts().map(transaction::getType).filter(t -> t != null).collect(toSet());
    }

    @Override
    public Set<String> getSelectedNames() {
        return names;
    }

    @Override
    public Pattern.Conjunction<Pattern.Admin> getPattern() {
        return pattern;
    }

    @Override
    public String toString() {
        String selectString = "";
        if (userDefinedNames) {
            selectString = "select " + names.stream().map(s -> "$" + s).collect(Collectors.joining(", "));
        }

        String modifiers = "";
        modifiers += limit.map(l -> "limit " + l + " ").orElse("");
        if (offset != 0) modifiers += "offset " + Long.toString(offset) + " ";
        if (distinct) modifiers += "distinct ";
        String orderResource = orderResourceType.map(r -> "(has " + r + ")").orElse("");
        modifiers += orderVar.map(v -> "order by $" + v + orderResource + " ").orElse("");

        return String.format("match %s %s%s", pattern, modifiers, selectString).trim();
    }

    /**
     * @return the query that will match the specified patterns
     */
    private Query getQuery() {
        return new Query(transaction, this.pattern);
    }

    /**
     * Apply query modifiers (limit, offset, distinct, order) to the graph traversal
     * @param traversal the graph traversal to apply modifiers to
     */
    private void applyModifiers(GraphTraversal<Vertex, Map<String, Vertex>> traversal) {
        orderTraversal(traversal);

        String[] namesArray = names.toArray(new String[names.size()]);

        // Must provide three arguments in order to pass an array to .select
        // If ordering, select the variable to order by as well
        if (orderVar.isPresent()) {
            traversal.select(orderVar.get(), orderVar.get(), namesArray);
        } else if (namesArray.length != 0) {
            traversal.select(namesArray[0], namesArray[0], namesArray);
        }
    }

    /**
     * @return get a comparator to order results based on what ordering is specified in the query
     */
    private Comparator<Map<String, Vertex>> resultComparator() {
        // Get the comparator to use for ordering final results
        Comparator<Map<String, Vertex>> comparator;
        
        if (orderResourceType.isPresent()) {
            // Order by resource type
            String typeId = orderResourceType.get();
            DataType.ConceptProperty valueProp = transaction.getResourceType(typeId).getDataType().getConceptProperty();

            comparator = Comparator.comparing(
                    result -> getResourceValue(result.get(orderVar.get()), typeId, valueProp),
                    new ResourceComparator()
            );
        } else {
            // Order by item identifier
            comparator = Comparator.comparing(result -> result.get(orderVar.get()).value(ITEM_IDENTIFIER.name()));
        }

        if (!orderAsc) comparator = comparator.reversed();

        return comparator;
    }

    /**
     * Order the traversal using the ordering specified in the query
     * @param traversal the traversal to order
     */
    private void orderTraversal(GraphTraversal<Vertex, Map<String, Vertex>> traversal) {
        if (orderVar.isPresent()) {
            if (orderResourceType.isPresent()) {
                // Order by resource type

                // Look up datatype of resource type
                String typeId = orderResourceType.get();
                DataType.ConceptProperty valueProp = transaction.getResourceType(typeId).getDataType().getConceptProperty();

                Comparator<Optional<Comparable>> comparator = new ResourceComparator();
                if (!orderAsc) comparator = comparator.reversed();

                traversal.select(orderVar.get()).order().by(v -> getResourceValue(v, typeId, valueProp), comparator);
            } else {
                // Order by ITEM_IDENTIFIER by default
                Order order = orderAsc ? Order.incr : Order.decr;
                traversal.select(orderVar.get()).order().by(ITEM_IDENTIFIER.name(), order);
            }
        }
    }

    /**
     * Get the value of an attached resource, used for ordering by resource
     * @param elem the element in the graph (a gremlin object)
     * @param resourceTypeId the ID of a resource type
     * @param value the VALUE property to use on the vertex
     * @return the value of an attached resource, or nothing if there is no resource of this type
     */
    private Optional<Comparable> getResourceValue(Object elem, String resourceTypeId, DataType.ConceptProperty value) {
        return ((MindmapsTransactionImpl) transaction).getTinkerPopGraph().traversal().V(elem)
                .outE(SHORTCUT.getLabel()).has(TO_TYPE.name(), resourceTypeId).inV().values(value.name())
                .tryNext().map(o -> (Comparable) o);
    }

    /**
     * @param vertices a map of vertices where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<String, Concept> makeResults(Map<String, Vertex> vertices) {
        Map<String, Concept> map = new HashMap<>();
        for (String name : names) {
            Vertex vertex = vertices.get(name);
            Concept concept = transaction.getConcept(vertex.value(ITEM_IDENTIFIER.name()));
            if(concept != null)
                map.put(name, concept);
        }
        return map;
        //TODO: Find out why lambda fails when Titan Indices don't return anything. I.e. when concept = null. This happens when Titan indices are corrupted.
        /*return names.stream().collect(Collectors.toMap(
                name -> name,
                name -> transaction.getConcept(vertices.get(name).value(ITEM_IDENTIFIER.name()))
        ));*/
    }

    /**
     * @param streams a collection of streams to mergesort
     * @param comparator a comparator to decide how to sort the elements of the stream
     * @param <T> the type of the elements in the stream
     * @return a single sorted stream, sorted using mergesort
     */
    private static <T> Stream<T> mergeSortedStreams(Collection<Stream<T>> streams, Comparator<T> comparator) {
        // Merge several sorted streams into one sorted streams
        List<Iterator<T>> iterators = streams.stream().map(Stream::iterator).collect(toList());
        UnmodifiableIterator<T> merged = Iterators.mergeSorted(iterators, comparator);
        Iterable<T> iterable = () -> merged;
        return StreamSupport.stream(iterable.spliterator(), true);
    }

    /**
     * Validate the MatchQuery, assuming a transaction has been specified
     */
    private void validate() {
        new MatchQueryValidator(admin()).validate(transaction);
    }

    /**
     * A comparator that parses (optionally present) resources into the correct datatype for comparison
     */
    static class ResourceComparator implements Comparator<Optional<Comparable>> {

        @Override
        public int compare(Optional<Comparable> value1, Optional<Comparable> value2) {
            if (!value1.isPresent() && !value2.isPresent()) return 0;
            if (!value1.isPresent()) return -1;
            if (!value2.isPresent()) return 1;

            //noinspection unchecked
            return value1.get().compareTo(value2.get());
        }
    }
}

