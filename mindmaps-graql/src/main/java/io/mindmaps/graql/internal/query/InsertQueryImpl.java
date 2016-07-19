package io.mindmaps.graql.internal.query;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.core.implementation.DataType;
import io.mindmaps.core.model.*;
import io.mindmaps.graql.api.query.InsertQuery;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.GraqlType;
import io.mindmaps.graql.internal.validation.ErrorMessage;
import io.mindmaps.graql.internal.validation.InsertQueryValidator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * A query that will insert a collection of variables into a graph
 */
public class InsertQueryImpl implements InsertQuery.Admin {

    private final MatchQuery matchQuery;
    private MindmapsTransaction transaction;
    private final Collection<Var.Admin> originalVars;
    private final Collection<Var.Admin> vars;
    private final Map<String, List<Var.Admin>> varsByName;
    private final Map<String, List<Var.Admin>> varsById;
    private Map<String, Concept> concepts = new HashMap<>();
    private Stack<String> visitedVars = new Stack<>();

    private RelationType hasResource = null;
    private RoleType hasResourceTarget = null;
    private RoleType hasResourceValue = null;

    /**
     * @param transaction the transaction to execute on
     * @param vars a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     */
    public InsertQueryImpl(MindmapsTransaction transaction, Collection<Var.Admin> vars, MatchQuery matchQuery) {
        this.transaction = transaction;

        this.originalVars = vars;
        this.matchQuery = matchQuery;

        // Get all variables, including ones nested in other variables
        this.vars = vars.stream().flatMap(v -> v.getInnerVars().stream()).collect(toSet());

        // Group variables by name
        varsByName = this.vars.stream().collect(Collectors.groupingBy(Var.Admin::getName));

        // Group variables by id (if they have one defined)
        varsById = this.vars.stream()
                .filter(var -> var.getId().isPresent())
                .collect(Collectors.groupingBy(var -> var.getId().get()));

        new InsertQueryValidator(this).validate(transaction);
    }

    /**
     * @param transaction the transaction to execute on
     * @param vars a collection of Vars to insert
     */
    public InsertQueryImpl(MindmapsTransaction transaction, Collection<Var.Admin> vars) {
        this(transaction, vars, null);
    }

    @Override
    public InsertQuery withTransaction(MindmapsTransaction transaction) {
        this.transaction = Objects.requireNonNull(transaction);
        return this;
    }

    @Override
    public void execute() {
        // Do nothing, just execute whole stream
        stream().forEach(c -> {});
    }

    @Override
    public Stream<Concept> stream() {
        if (transaction == null) throw new IllegalStateException(ErrorMessage.NO_TRANSACTION.getMessage());

        if (matchQuery == null) {
            concepts.clear();
            return insertAll();
        } else {
            return matchQuery.stream().flatMap(
                    resultMap -> {
                        concepts = new HashMap<>(resultMap);
                        return insertAll();
                    }
            );
        }
    }

    @Override
    public Admin admin() {
        return this;
    }

    @Override
    public Optional<MatchQuery> getMatchQuery() {
        return Optional.ofNullable(matchQuery);
    }

    @Override
    public Collection<Var.Admin> getVars() {
        return originalVars;
    }

    @Override
    public Collection<Var.Admin> getAllVars() {
        return vars;
    }

    /**
     * Insert all the Vars
     */
    private Stream<Concept> insertAll() {
        return getAllVars().stream().map(this::insertVar);
    }

    /**
     * @param var the Var to insert into the graph
     */
    private Concept insertVar(Var.Admin var) {
        Concept concept = getConcept(var);

        if (var.getAbstract()) concept.asType().setAbstract(true);

        setValue(var);

        var.getLhs().ifPresent(lhs -> concept.asRule().setLHS(lhs));
        var.getRhs().ifPresent(rhs -> concept.asRule().setRHS(rhs));

        var.getHasRoles().stream().forEach(role -> concept.asRelationType().hasRole(getConcept(role).asRoleType()));
        var.getPlaysRoles().stream().forEach(role -> concept.asType().playsRole(getConcept(role).asRoleType()));
        var.getScopes().stream().forEach(scope -> concept.asRelation().scope(getConcept(scope).asInstance()));

        if (!var.getResourceEqualsPredicates().isEmpty()) {
            Instance instance = concept.asInstance();
            var.getResourceEqualsPredicates().forEach((type, values) -> addResources(instance, type, values));
        }

        var.getCastings().forEach(casting -> addCasting(var, casting));

        return concept;
    }

    /**
     * @param var the Var that is represented by a concept in the graph
     * @return the same as addConcept, but using an internal map to remember previous calls
     */
    private Concept getConcept(Var.Admin var) {
        String name = var.getName();
        if (visitedVars.contains(name)) {
            throw new IllegalStateException(ErrorMessage.INSERT_RECURSIVE.getMessage(var.getPrintableName()));
        }

        visitedVars.push(name);
        Concept concept = concepts.computeIfAbsent(name, n -> addConcept(var));
        visitedVars.pop();
        return concept;
    }

    /**
     * @param var the Var that is to be added into the graph
     * @return the concept representing the given Var, creating it if it doesn't exist
     */
    private Concept addConcept(Var.Admin var) {
        var = mergeVar(var);

        Optional<Var.Admin> type = var.getType();
        Optional<Var.Admin> ako = var.getAko();

        if (type.isPresent() && ako.isPresent()) {
            String printableName = var.getPrintableName();
            throw new IllegalStateException(ErrorMessage.INSERT_ISA_AND_AKO.getMessage(printableName));
        }

        // Get explicit id or random UUID
        String id = var.getId().orElse(UUID.randomUUID().toString());

        Concept concept;

        // If 'ako' provided, use that, else use 'isa', else get existing concept by id
        if (ako.isPresent()) {
            concept = putConceptBySuperType(id, getConcept(ako.get()).asType());
        } else if (type.isPresent()) {
            concept = putConceptByType(id, var, getConcept(type.get()).asType());
        } else {
            concept = transaction.getConcept(id);
        }

        if (concept == null) {
            System.out.println(varsById);
            throw new IllegalStateException(
                    var.getId().map(ErrorMessage.INSERT_GET_NON_EXISTENT_ID::getMessage)
                            .orElse(ErrorMessage.INSERT_UNDEFINED_VARIABLE.getMessage(var.getName()))
            );
        }

        return concept;
    }

    /**
     * Merge a variable with any other variables referred to with the same variable name or id
     * @param var the variable to merge
     * @return the merged variable
     */
    private Var.Admin mergeVar(Var.Admin var) {
        boolean changed = true;
        Set<Var.Admin> varsToMerge = new HashSet<>();

        // Keep merging until the set of merged variables stops changing
        // This handles cases when variables are referred to with multiple degrees of separation
        // e.g.
        // "123" isa movie; $x id "123"; $y id "123"; ($y, $z)
        while (changed) {
            // Merge variable referred to by name...
            boolean byNameChange = varsToMerge.addAll(varsByName.get(var.getName()));
            var = new VarImpl(varsToMerge);

            // Then merge variables referred to by id...
            boolean byIdChange = var.getId().map(id -> varsToMerge.addAll(varsById.get(id))).orElse(false);
            var = new VarImpl(varsToMerge);

            changed = byNameChange | byIdChange;
        }

        return var;
    }

    /**
     * @param id the ID of the concept
     * @param var the Var representing the concept in the insert query
     * @param type the type of the concept
     * @return a concept with the given ID and the specified type
     */
    private Concept putConceptByType(String id, Var.Admin var, Type type) {
        String typeId = type.getId();
        if (typeId.equals(DataType.ConceptMeta.ENTITY_TYPE.getId())) {
            return transaction.putEntityType(id);
        } else if (typeId.equals(DataType.ConceptMeta.RELATION_TYPE.getId())) {
            return transaction.putRelationType(id);
        } else if (typeId.equals(DataType.ConceptMeta.ROLE_TYPE.getId())) {
            return transaction.putRoleType(id);
        } else if (typeId.equals(DataType.ConceptMeta.RESOURCE_TYPE.getId())) {
            return transaction.putResourceType(id, getDataType(var));
        } else if (typeId.equals(DataType.ConceptMeta.RULE_TYPE.getId())) {
            return transaction.putRuleType(id);
        } else if (type.isEntityType()) {
            return transaction.putEntity(id, type.asEntityType());
        } else if (type.isRelationType()) {
            return transaction.putRelation(id, type.asRelationType());
        } else if (type.isResourceType()) {
            return transaction.putResource(id, type.asResourceType());
        } else if (type.isRuleType()) {
            return transaction.putRule(id, type.asRuleType());
        } else {
            throw new RuntimeException("Unrecognized type " + type.getId());
        }
    }

    /**
     * @param id the ID of the concept
     * @param superType the supertype of the concept
     * @return a concept with the given ID and the specified supertype
     */
    private <T> Concept putConceptBySuperType(String id, Type superType) {
        if (superType.isEntityType()) {
            return transaction.putEntityType(id).superType(superType.asEntityType());
        } else if (superType.isRelationType()) {
            return transaction.putRelationType(id).superType(superType.asRelationType());
        } else if (superType.isRoleType()) {
            return transaction.putRoleType(id).superType(superType.asRoleType());
        } else if (superType.isResourceType()) {
            ResourceType<T> superResource = superType.asResourceType();
            return transaction.putResourceType(id, superResource.getDataType()).superType(superResource);
        } else if (superType.isRuleType()) {
            return transaction.putRuleType(id).superType(superType.asRuleType());
        } else {
            throw new IllegalStateException(ErrorMessage.INSERT_METATYPE.getMessage(id, superType.getId()));
        }
    }

    /**
     * Add a roleplayer to the given relation
     * @param var the variable representing the relation
     * @param casting a casting between a role type and role player
     */
    private void addCasting(Var.Admin var, Var.Casting casting) {
        Relation relation = getConcept(var).asRelation();

        RoleType roleType = getConcept(casting.getRoleType().get()).asRoleType();
        Instance roleplayer = getConcept(casting.getRolePlayer()).asInstance();
        relation.putRolePlayer(roleType, roleplayer);
    }

    /**
     * Set the values specified in the Var, on the concept represented by the given Var
     * @param var the Var containing values to set on the concept
     */
    private void setValue(Var.Admin var) {
        Iterator<?> values = var.getValueEqualsPredicates().iterator();

        if (values.hasNext()) {
            Object value = values.next();

            if (values.hasNext()) {
                throw new IllegalStateException(ErrorMessage.INSERT_MULTIPLE_VALUES.getMessage(value, values.next()));
            }

            Concept concept = getConcept(var);

            if (concept.isType()) {
                concept.asType().setValue((String) value);
            } else if (concept.isEntity()) {
                concept.asEntity().setValue((String) value);
            } else if (concept.isRelation()) {
                concept.asRelation().setValue((String) value);
            } else if (concept.isRule()) {
                concept.asRule().setValue((String) value);
            } else if (concept.isResource()) {
                // If the value we provide is not supported by this resource, core will throw an exception back
                //noinspection unchecked
                concept.asResource().setValue(value);
            } else {
                throw new RuntimeException("Unrecognized type " + concept.type().getId());
            }
        }
    }

    /**
     * Get the datatype of a Var if specified, else throws an IllegalStateException
     * @return the datatype of the given var
     */
    private Data<?> getDataType(Var.Admin var) {
        return var.getDatatype().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.INSERT_NO_DATATYPE.getMessage(var.getPrintableName()))
        );
    }

    /**
     * Add resources to the given instance, using the has-resource relation
     * @param instance the instance to add resources to
     * @param resourceType the variable representing the resource type
     * @param values a set of values to set on the resource instances
     * @param <D> the type of the resources
     */
    private <D> void addResources(Instance instance, Var.Admin resourceType, Set<D> values) {
        // We assume the resource type has the correct datatype. If it does not, core will catch the problem
        //noinspection unchecked
        ResourceType<D> type = getConcept(resourceType).asResourceType();
        values.forEach(value -> addResource(instance, type, value));
    }

    /**
     * Add a resource to the given instance, using the has-resource relation
     * @param instance the instance to add a resource to
     * @param type the resource type
     * @param value the value to set on the resource instance
     * @param <D> the type of the resource
     */
    private <D> void addResource(Instance instance, ResourceType<D> type, D value) {
        Resource resource = transaction.addResource(type).setValue(value);

        if (hasResource == null) hasResource = transaction.getRelationType(GraqlType.HAS_RESOURCE);
        if (hasResourceTarget == null) hasResourceTarget = transaction.getRoleType(GraqlType.HAS_RESOURCE_TARGET);
        if (hasResourceValue == null) hasResourceValue = transaction.getRoleType(GraqlType.HAS_RESOURCE_VALUE);

        if (hasResource == null || hasResourceTarget == null || hasResourceValue == null) {
            throw new IllegalStateException(ErrorMessage.INSERT_NO_RESOURCE_RELATION.getMessage());
        }

        Relation relation = transaction.addRelation(hasResource);
        relation.putRolePlayer(hasResourceTarget, instance);
        relation.putRolePlayer(hasResourceValue, resource);
    }

    @Override
    public String toString() {
        return "insert " + originalVars.stream().map(v -> v + "; ").collect(Collectors.joining()).trim();
    }
}
