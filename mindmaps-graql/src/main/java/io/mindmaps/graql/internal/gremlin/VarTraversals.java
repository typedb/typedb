/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.gremlin;

import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static io.mindmaps.graql.Graql.eq;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.*;
import static io.mindmaps.util.Schema.ConceptProperty.*;
import static io.mindmaps.util.Schema.ConceptPropertyUnique.ITEM_IDENTIFIER;
import static io.mindmaps.util.Schema.EdgeLabel.*;
import static io.mindmaps.util.Schema.EdgeProperty.TO_TYPE;

/**
 * A collection of {@code MultiTraversals} that describe a {@code Var}.
 * <p>
 * A {@code VarTraversals} is constructed from a {@code Var} and produces several {@code MultiTraversals} that are used
 * by {@code Query} to produce gremlin traversals.
 * <p>
 * If possible, the {@code VarTraversals} will be represented using a {@code ShortcutTraversal}.
 */
public class VarTraversals {

    private final VarAdmin var;
    private final ShortcutTraversal shortcutTraversal = new ShortcutTraversal();
    private final Collection<MultiTraversal> traversals = new HashSet<>();
    private final Collection<VarTraversals> innerVarTraversals = new HashSet<>();
    private final Collection<String> castingVars = new HashSet<>();

    /**
     * Create VarTraversals to represent a Var
     * @param var the variable that this VarTraversal will represent
     */
    public VarTraversals(VarAdmin var) {
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
            Schema.ConceptProperty value = getValuePropertyForPredicate(predicate);
            addPropertyPattern(getName(), value.name(), predicate, getValuePriority(predicate));
        });

        // Check all predicates on RULE_LHS (for rules)
        var.getLhs().ifPresent(lhs -> {
            shortcutTraversal.setInvalid();
            addPropertyPattern(getName(), Schema.ConceptProperty.RULE_LHS.name(), eq(lhs).admin(), VALUE_NONSPECIFIC);
        });

        // Check all predicates on RULE_RHS (for rules)
        var.getRhs().ifPresent(rhs -> {
            shortcutTraversal.setInvalid();
            addPropertyPattern(getName(), Schema.ConceptProperty.RULE_RHS.name(), eq(rhs).admin(), VALUE_NONSPECIFIC);
        });

        var.getResourcePredicates().forEach((type, predicates) -> {
            // Currently it is guaranteed that resource types are specified with an ID
            //noinspection OptionalGetWithoutIsPresent
            String typeId = type.getId().get();

            if (predicates.isEmpty()) {
                // Check that a resource of the specified type is connected
                addResourcePattern(typeId);
            } else {
                // Check all predicates on connected resource's VALUE
                predicates.forEach(predicate -> addResourcePattern(typeId, predicate));
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
            Optional<VarAdmin> roleType = casting.getRoleType();
            if (roleType.isPresent()) {
                addRelatesPattern(roleType.get(), casting.getRolePlayer());
            } else {
                addRelatesPattern(casting.getRolePlayer());
            }
        });
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
        traversals.add(new MultiTraversalImpl(fragments));
    }

    /**
     * Add a pattern checking this variable has a VALUE property
     */
    private void addHasValuePattern() {
        addPattern(new FragmentImpl(
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
            String name, String property, ValuePredicateAdmin predicate, FragmentPriority priority
    ) {
        addPattern(new FragmentImpl(t -> t.has(property, predicate.getPredicate()), priority, name));
    }

    /**
     * Add a pattern checking that this variable has a resource of the given type
     * @param typeId the resource type ID
     * @return the variable name of the resource
     */
    private String addResourcePattern(String typeId) {
        shortcutTraversal.setInvalid();

        String resource = UUID.randomUUID().toString();

        addPattern(
                new FragmentImpl(t ->
                        t.outE(SHORTCUT.getLabel()).has(TO_TYPE.name(), typeId).inV(),
                        EDGE_UNBOUNDED, getName(), resource
                ),
                new FragmentImpl(t ->
                        t.inE(SHORTCUT.getLabel()).has(TO_TYPE.name(), typeId).outV(),
                        EDGE_UNBOUNDED, resource, getName()
                )
        );

        return resource;
    }

    /**
     * Add a pattern checking this variable has a resource of the given type matching the given predicate
     * @param typeId the resource type ID
     * @param predicate a predicate to match on a resource's VALUE
     */
    private void addResourcePattern(String typeId, ValuePredicateAdmin predicate) {
        String resource = addResourcePattern(typeId);
        Schema.ConceptProperty value = getValuePropertyForPredicate(predicate);
        addPropertyPattern(resource, value.name(), predicate, getValuePriority(predicate));
    }

    /**
     * Add a pattern confirming that an edge of the given label leads to something matching the given Var
     * @param edgeLabel the edge label name to follow
     * @param var the variable expected at the end of the edge
     */
    @SuppressWarnings("unchecked")
    private void addEdgePattern(Schema.EdgeLabel edgeLabel, VarAdmin var) {
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
                    new FragmentImpl(t -> t
                            .union(__.identity(), __.repeat(__.out(AKO.getLabel())).emit()).unfold()
                            .out(ISA.getLabel())
                            .union(__.identity(), __.repeat(__.out(AKO.getLabel())).emit()).unfold(),
                            getEdgePriority(edgeLabel, true), getName(), other
                    ),
                    new FragmentImpl(t -> t
                            .union(__.identity(), __.repeat(__.in(AKO.getLabel())).emit()).unfold()
                            .in(ISA.getLabel())
                            .union(__.identity(), __.repeat(__.in(AKO.getLabel())).emit()).unfold(),
                            getEdgePriority(edgeLabel, false), other, getName()
                    )
            );
        } else if (edgeLabel == AKO) {
            // Traverse inferred 'ako' edges
            addPattern(
                    new FragmentImpl(
                            t -> t.union(__.identity(), __.repeat(__.out(AKO.getLabel())).emit()).unfold(),
                            getEdgePriority(edgeLabel, true), getName(), other
                    ),
                    new FragmentImpl(
                            t -> t.union(__.identity(), __.repeat(__.in(AKO.getLabel())).emit()).unfold(),
                            getEdgePriority(edgeLabel, false), other, getName()
                    )
            );
        } else {
            String edge = edgeLabel.getLabel();
            addPattern(
                    new FragmentImpl(t -> t.out(edge), getEdgePriority(edgeLabel, true), getName(), other),
                    new FragmentImpl(t -> t.in(edge), getEdgePriority(edgeLabel, false), other, getName())
            );
        }

        innerVarTraversals.add(new VarTraversals(var));
    }

    /**
     * Add some patterns where this variable is a relation and the given variable is a roleplayer of that relation
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private void addRelatesPattern(VarAdmin rolePlayer) {
        String other = rolePlayer.getName();
        shortcutTraversal.addRel(other);

        String casting = makeDistinctCastingVar();

        // Pattern between relation and casting
        addPattern(
                new FragmentImpl(t -> t.out(CASTING.getLabel()), EDGE_BOUNDED, getName(), casting),
                new FragmentImpl(t -> t.in(CASTING.getLabel()), EDGE_UNBOUNDED, casting, getName())
        );

        // Pattern between casting and roleplayer
        addPattern(
                new FragmentImpl(t -> t.out(ROLE_PLAYER.getLabel()), EDGE_UNIQUE, casting, other),
                new FragmentImpl(t -> t.in(ROLE_PLAYER.getLabel()), EDGE_BOUNDED, other, casting)
        );

        innerVarTraversals.add(new VarTraversals(rolePlayer));
    }

    /**
     * Add some patterns where this variable is a relation relating the given roleplayer as the given roletype
     * @param roleType a variable that is the roletype of the given roleplayer
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private void addRelatesPattern(VarAdmin roleType, VarAdmin rolePlayer) {
        String roletypeName = roleType.getName();
        String roleplayerName = rolePlayer.getName();

        VarTraversals roletypePattern = new VarTraversals(roleType);

        Optional<String> roleTypeId = roleType.getIdOnly();

        if (roleTypeId.isPresent()) {
            roletypePattern.getTraversals().forEach(traversals::add);
            shortcutTraversal.addRel(roleTypeId.get(), rolePlayer.getName());
        } else {
            innerVarTraversals.add(roletypePattern);
            shortcutTraversal.setInvalid();
        }

        String casting = makeDistinctCastingVar();

        // Pattern between relation and casting
        addPattern(
                new FragmentImpl(t -> t.out(CASTING.getLabel()), EDGE_BOUNDED, getName(), casting),
                new FragmentImpl(t -> t.in(CASTING.getLabel()), EDGE_UNBOUNDED, casting, getName())
        );

        // Pattern between casting and roleplayer
        addPattern(
                new FragmentImpl(t -> t.out(ROLE_PLAYER.getLabel()), EDGE_UNIQUE, casting, roleplayerName),
                new FragmentImpl(t -> t.in(ROLE_PLAYER.getLabel()), EDGE_BOUNDED, roleplayerName, casting)
        );

        // Pattern between casting and role type
        addPattern(
                new FragmentImpl(t -> t.out(ISA.getLabel()), EDGE_UNIQUE, casting, roletypeName),
                new FragmentImpl(t -> t.in(ISA.getLabel()), EDGE_UNBOUNDED, roletypeName, casting)
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
    private void addHasResourcePattern(VarAdmin type) {
        String typeId = type.getId().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_ID_SPECIFIED_FOR_HAS_RESOURCE.getMessage())
        );

        Var owner = Graql.id(GraqlType.HAS_RESOURCE_OWNER.getId(typeId))
                .isa(Schema.MetaType.ROLE_TYPE.getId());
        Var value = Graql.id(GraqlType.HAS_RESOURCE_VALUE.getId(typeId))
                .isa(Schema.MetaType.ROLE_TYPE.getId());

        Var relationType = Graql.id(GraqlType.HAS_RESOURCE.getId(typeId))
                .isa(Schema.MetaType.RELATION_TYPE.getId())
                .hasRole(owner).hasRole(value);

        addEdgePattern(PLAYS_ROLE, owner.admin());

        VarTraversals relationTraversals = new VarTraversals(type);
        relationTraversals.addEdgePattern(PLAYS_ROLE, value.admin());

        innerVarTraversals.add(new VarTraversals(relationType.admin()));
        innerVarTraversals.add(relationTraversals);
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
        return new MultiTraversalImpl(new FragmentImpl(t -> t.where(P.neq(otherCastingId)), DISTINCT_CASTING, casting));
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
    private Schema.ConceptProperty getValuePropertyForPredicate(ValuePredicateAdmin predicate) {
        Object value = predicate.getInnerValues().iterator().next();
        return ResourceType.DataType.SUPPORTED_TYPES.get(value.getClass().getTypeName()).getConceptProperty();
    }
}
