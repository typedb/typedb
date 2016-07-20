package io.mindmaps.graql.internal.gremlin;

import io.mindmaps.core.implementation.Data;
import io.mindmaps.core.implementation.DataType;
import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.ValuePredicate;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.GraqlType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static io.mindmaps.core.implementation.DataType.ConceptProperty.*;
import static io.mindmaps.core.implementation.DataType.ConceptPropertyUnique.ITEM_IDENTIFIER;
import static io.mindmaps.core.implementation.DataType.EdgeLabel.*;
import static io.mindmaps.core.implementation.DataType.EdgeProperty.TO_TYPE;
import static io.mindmaps.graql.api.query.ValuePredicate.eq;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.*;

/**
 * A collection of {@code MultiTraversals} that describe a {@code Var}.
 * <p>
 * A {@code VarTraversals} is constructed from a {@code Var} and produces several {@code MultiTraversals} that are used
 * by {@code Query} to produce gremlin traversals.
 * <p>
 * If possible, the {@code VarTraversals} will be represented using a {@code ShortcutTraversal}.
 */
public class VarTraversals {

    private final Var.Admin var;
    private final ShortcutTraversal shortcutTraversal = new ShortcutTraversal();
    private final Collection<MultiTraversal> traversals = new HashSet<>();
    private final Collection<VarTraversals> innerVarTraversals = new HashSet<>();
    private final Collection<String> castingVars = new HashSet<>();

    /**
     * Create VarTraversals to represent a Var
     * @param var the variable that this VarTraversal will represent
     */
    public VarTraversals(Var.Admin var) {
        this.var = var;

        // If the user has provided a variable name, it can't be represented with a shortcut edge because it may be
        // referred to later.
        if (var.isUserDefinedName()) {
            shortcutTraversal.setInvalid();
        }

        // Check the IS_ABSTRACT property
        if (var.getAbstract()) {
            addPropertyPattern(getName(), IS_ABSTRACT.name(), eq(true).admin(), EDGE_UNBOUNDED);
        }

        // Check the DATA_TYPE property
        var.getDatatype().ifPresent(
                datatype -> addPropertyPattern(getName(), DATA_TYPE.name(), eq(datatype.getName()).admin(), VALUE_NONSPECIFIC)
        );

        // Check ITEM_IDENTIFIER
        var.getId().ifPresent(
                id -> {
                    shortcutTraversal.setInvalid();
                    addPropertyPattern(getName(), ITEM_IDENTIFIER.name(), eq(id).admin(), ID);
                }
        );

        // Verify the concept has a VALUE
        if (var.hasValue()) {
            addHasValuePattern();
        }

        // Check all predicates on VALUE
        var.getValuePredicates().forEach(predicate -> {
            shortcutTraversal.setInvalid();
            DataType.ConceptProperty value = getValuePropertyForPredicate(predicate);
            addPropertyPattern(getName(), value.name(), predicate, getValuePriority(predicate));
        });

        // Check all predicates on RULE_LHS (for rules)
        var.getLhs().ifPresent(lhs -> {
            shortcutTraversal.setInvalid();
            addPropertyPattern(getName(), DataType.ConceptProperty.RULE_LHS.name(), eq(lhs).admin(), VALUE_NONSPECIFIC);
        });

        // Check all predicates on RULE_RHS (for rules)
        var.getRhs().ifPresent(rhs -> {
            shortcutTraversal.setInvalid();
            addPropertyPattern(getName(), DataType.ConceptProperty.RULE_RHS.name(), eq(rhs).admin(), VALUE_NONSPECIFIC);
        });

        var.getResourcePredicates().forEach((type, predicates) -> {
            if (predicates.isEmpty()) {
                // Check that a resource of the specified type is connected
                addResourcePattern(type);
            } else {
                // Check all predicates on connected resource's VALUE
                predicates.forEach(predicate -> addResourcePattern(type, predicate));
            }
        });

        // Check identity of outgoing ISA, AKO, HAS_ROLE, PLAYS_ROLE and HAS_SCOPE edges
        var.getType().ifPresent(type -> addEdgePattern(ISA, type));
        var.getAko().ifPresent(type -> addEdgePattern(AKO, type));
        var.getHasRoles().forEach(type -> addEdgePattern(HAS_ROLE, type));
        var.getPlaysRoles().forEach(type -> addEdgePattern(PLAYS_ROLE, type));
        var.getScopes().forEach(type -> addEdgePattern(HAS_SCOPE, type));
        var.getHasResourceTypes().forEach(this::addHasResourcePattern);

        // Check identity of roleplayers and role types (if specified)
        var.getCastings().forEach(casting -> {
            if (casting.getRoleType().isPresent()) {
                addRelatesPattern(casting.getRoleType().get(), casting.getRolePlayer());
            } else {
                addRelatesPattern(casting.getRolePlayer());
            }
        });

        // If this is a type, confirm that its ITEM_IDENTIFIER is correct
        var.getId().ifPresent(this::addTypeNamePattern);
    }

    /**
     * @return a stream of traversals describing the variable
     */
    public Stream<MultiTraversal> getTraversals() {
        Stream<MultiTraversal> myPatterns;
        Stream<MultiTraversal> innerPatterns = innerVarTraversals.stream().flatMap(VarTraversals::getTraversals);

        if (shortcutTraversal.isValid()) {
            myPatterns = Stream.of(shortcutTraversal.getMultiTraversal());
        } else {
            myPatterns = traversals.stream();
        }

        return Stream.concat(myPatterns, innerPatterns);
    }

    /**
     * Helper function to add a MultiTraversal containing the given fragments.
     * @param fragments some Fragments to put into a MultiTraversal and attach to this object
     */
    private void addPattern(Fragment... fragments) {
        traversals.add(new MultiTraversal(fragments));
    }

    /**
     * Add a pattern checking this variable has a VALUE property
     */
    private void addHasValuePattern() {
        addPattern(new Fragment(
                t -> t.or(
                        __.has(VALUE_STRING.name()),
                        __.has(VALUE_LONG.name()),
                        __.has(VALUE_DOUBLE.name()),
                        __.has(VALUE_BOOLEAN.name())),
                VALUE_NONSPECIFIC, getName()
        ));
    }

    /**
     * Add a pattern checking that this variable has a property matching the given predicate
     * @param name the name of the variable
     * @param property the property name on the vertex
     * @param predicate the predicate to match against the property's value
     * @param priority the priority of the fragment
     */
    private void addPropertyPattern(
            String name, String property, ValuePredicate.Admin predicate, FragmentPriority priority
    ) {
        addPattern(new Fragment(t -> t.has(property, predicate.getPredicate()), priority, name));
    }

    /**
     * Add a pattern checking that this variable has a resource of the given type
     * @param type a variable representing the resource type
     * @return the variable name of the resource
     */
    private String addResourcePattern(Var.Admin type) {
        shortcutTraversal.setInvalid();

        String resource = UUID.randomUUID().toString();

        addPattern(
                new Fragment(t ->
                        t.outE(SHORTCUT.getLabel()).has(TO_TYPE.name(), type.getId().get()).inV(),
                        EDGE_UNBOUNDED, getName(), resource
                ),
                new Fragment(t ->
                        t.inE(SHORTCUT.getLabel()).has(TO_TYPE.name(), type.getId().get()).outV(),
                        EDGE_UNBOUNDED, resource, getName()
                )
        );

        return resource;
    }

    /**
     * Add a pattern checking this variable has a resource of the given type matching the given predicate
     * @param type a variable representing the resource type
     * @param predicate a predicate to match on a resource's VALUE
     */
    private void addResourcePattern(Var.Admin type, ValuePredicate.Admin predicate) {
        String resource = addResourcePattern(type);
        DataType.ConceptProperty value = getValuePropertyForPredicate(predicate);
        addPropertyPattern(resource, value.name(), predicate, getValuePriority(predicate));
    }

    /**
     * Add a pattern confirming that an edge of the given label leads to something matching the given Var
     * @param edgeLabel the edge label name to follow
     * @param var the variable expected at the end of the edge
     */
    @SuppressWarnings("unchecked")
    private void addEdgePattern(DataType.EdgeLabel edgeLabel, Var.Admin var) {
        String other = var.getName();
        Optional<String> typeName = var.getId();

        if (edgeLabel == ISA && typeName.isPresent()) {
            shortcutTraversal.setType(typeName.get());
        } else {
            shortcutTraversal.setInvalid();
        }

        if (edgeLabel == ISA) {
            // Traverse inferred 'isa's by 'ako' edges
            addPattern(
                    new Fragment(t -> t
                            .union(__.identity(), __.repeat(__.out(AKO.getLabel())).emit()).unfold()
                            .out(ISA.getLabel())
                            .union(__.identity(), __.repeat(__.out(AKO.getLabel())).emit()).unfold(),
                            getEdgePriority(edgeLabel, true), getName(), other
                    ),
                    new Fragment(t -> t
                            .union(__.identity(), __.repeat(__.in(AKO.getLabel())).emit()).unfold()
                            .in(ISA.getLabel())
                            .union(__.identity(), __.repeat(__.in(AKO.getLabel())).emit()).unfold(),
                            getEdgePriority(edgeLabel, false), other, getName()
                    )
            );
        } else if (edgeLabel == AKO) {
            // Traverse inferred 'ako' edges
            addPattern(
                    new Fragment(
                            t -> t.union(__.identity(), __.repeat(__.out(AKO.getLabel())).emit()).unfold(),
                            getEdgePriority(edgeLabel, true), getName(), other
                    ),
                    new Fragment(
                            t -> t.union(__.identity(), __.repeat(__.in(AKO.getLabel())).emit()).unfold(),
                            getEdgePriority(edgeLabel, false), other, getName()
                    )
            );
        } else {
            String edge = edgeLabel.getLabel();
            addPattern(
                    new Fragment(t -> t.out(edge), getEdgePriority(edgeLabel, true), getName(), other),
                    new Fragment(t -> t.in(edge), getEdgePriority(edgeLabel, false), other, getName())
            );
        }

        innerVarTraversals.add(new VarTraversals(var));
    }

    /**
     * Add some patterns where this variable is a relation and the given variable is a roleplayer of that relation
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private void addRelatesPattern(Var.Admin rolePlayer) {
        String other = rolePlayer.getName();
        shortcutTraversal.addRel(other);

        String casting = makeDistinctCastingVar();

        // Pattern between relation and casting
        addPattern(
                new Fragment(t -> t.out(CASTING.getLabel()), EDGE_BOUNDED, getName(), casting),
                new Fragment(t -> t.in(CASTING.getLabel()), EDGE_UNBOUNDED, casting, getName())
        );

        // Pattern between casting and roleplayer
        addPattern(
                new Fragment(t -> t.out(ROLE_PLAYER.getLabel()), EDGE_UNIQUE, casting, other),
                new Fragment(t -> t.in(ROLE_PLAYER.getLabel()), EDGE_BOUNDED, other, casting)
        );

        innerVarTraversals.add(new VarTraversals(rolePlayer));
    }

    /**
     * Add some patterns where this variable is a relation relating the given roleplayer as the given roletype
     * @param roleType a variable that is the roletype of the given roleplayer
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private void addRelatesPattern(Var.Admin roleType, Var.Admin rolePlayer) {
        String roletypeName = roleType.getName();
        String roleplayerName = rolePlayer.getName();

        VarTraversals roletypePattern = new VarTraversals(roleType);

        if (roleType.getIdOnly().isPresent()) {
            roletypePattern.getTraversals().forEach(traversals::add);
            shortcutTraversal.addRel(roleType.getIdOnly().get(), rolePlayer.getName());
        } else {
            innerVarTraversals.add(roletypePattern);
            shortcutTraversal.setInvalid();
        }

        String casting = makeDistinctCastingVar();

        // Pattern between relation and casting
        addPattern(
                new Fragment(t -> t.out(CASTING.getLabel()), EDGE_BOUNDED, getName(), casting),
                new Fragment(t -> t.in(CASTING.getLabel()), EDGE_UNBOUNDED, casting, getName())
        );

        // Pattern between casting and roleplayer
        addPattern(
                new Fragment(t -> t.out(ROLE_PLAYER.getLabel()), EDGE_UNIQUE, casting, roleplayerName),
                new Fragment(t -> t.in(ROLE_PLAYER.getLabel()), EDGE_BOUNDED, roleplayerName, casting)
        );

        // Pattern between casting and role type
        addPattern(
                new Fragment(t -> t.out(ISA.getLabel()), EDGE_UNIQUE, casting, roletypeName),
                new Fragment(t -> t.in(ISA.getLabel()), EDGE_UNBOUNDED, roletypeName, casting)
        );

        innerVarTraversals.add(new VarTraversals(rolePlayer));
    }

    /**
     * Add patterns for the 'has-resource' syntax. This must check for a structure like so:
     *
     * {@code
     * [this] -plays-role-> has-[type]-owner <-has-role- has-[type] -has-role-> has-[type]-value <-plays-role- [type]
     * }
     *
     * @param type the resource type variable
     */
    private void addHasResourcePattern(Var.Admin type) {
        Var owner = QueryBuilder.id(GraqlType.HAS_RESOURCE_OWNER.getId(type.getId().get()))
                .isa(DataType.ConceptMeta.ROLE_TYPE.getId());
        Var value = QueryBuilder.id(GraqlType.HAS_RESOURCE_VALUE.getId(type.getId().get()))
                .isa(DataType.ConceptMeta.ROLE_TYPE.getId());

        Var relationType = QueryBuilder.id(GraqlType.HAS_RESOURCE.getId(type.getId().get()))
                .isa(DataType.ConceptMeta.RELATION_TYPE.getId())
                .hasRole(owner).hasRole(value);

        addEdgePattern(PLAYS_ROLE, owner.admin());

        VarTraversals relationTraversals = new VarTraversals(type);
        relationTraversals.addEdgePattern(PLAYS_ROLE, value.admin());

        innerVarTraversals.add(new VarTraversals(relationType.admin()));
        innerVarTraversals.add(relationTraversals);
    }

    /**
     * Add a pattern checking the ITEM_IDENTIFIER of the given type
     * @param typeName the expected ITEM_IDENTIFIER of the variable
     */
    private void addTypeNamePattern(String typeName) {
        addPattern(
                new Fragment(t -> t.has(ITEM_IDENTIFIER.name(), typeName), TYPE_ID, getName())
        );
    }

    /**
     * Create a variable name to represent a casting and add patterns to indicate this casting is distinct from other
     * castings.
     * @return the variable name of a new variable that will represent a casting.
     */
    private String makeDistinctCastingVar() {
        String casting = UUID.randomUUID().toString();

        // Assert that all castings are distinct in this relationship
        castingVars.stream()
                .map(otherCastingId -> makeDistinctCastingPattern(casting, otherCastingId))
                .forEach(traversals::add);

        castingVars.add(casting);

        return casting;
    }

    /**
     * @param casting a casting variable name
     * @param otherCastingId a different casting variable name
     * @return a MultiTraversal that indicates two castings are unique
     */
    private MultiTraversal makeDistinctCastingPattern(String casting, String otherCastingId) {
        return new MultiTraversal(new Fragment(t -> t.where(P.neq(otherCastingId)), DISTINCT_CASTING, casting));
    }

    /**
     * @return the name of the variable
     */
    private String getName() {
        return var.getName();
    }

    /**
     * @param predicate a predicate to test on a vertex
     * @return the correct VALUE property to check on the vertex for the given predicate
     */
    private DataType.ConceptProperty getValuePropertyForPredicate(ValuePredicate.Admin predicate) {
        Object value = predicate.getInnerValues().iterator().next();
        return Data.SUPPORTED_TYPES.get(value.getClass().getTypeName()).getConceptProperty();
    }
}
