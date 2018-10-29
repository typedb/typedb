/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.reasoner;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TestQueryPattern {

    public abstract List<String> patternList(Concept... args);


    /**
     * Generates different relation patterns variants as a cartesian products of provided id configurations.
     *
     * ( (role label): $x, (role label): $y, ...) {T1(x), T2(y), ...}, {[x/...], [y/...], ...}
     *
     * NB: only roleplayer ids are involved in the Cartesian Product
     *
     * Example:
     * [someRole, someType, {V123, V456, V789} ]
     * [anotherRole, x, {V123}]
     * [yetAnotherRole, x, {V456}]
     *
     * Will generate the following relations:
     *
     * (someRole: $genVarA, anotherRole: $genVarB, yetAnotherRole: $genVarC), someType($genVarA)
     * (someRole: $genVarA, anotherRole: $genVarB, yetAnotherRole: $genVarC), [$genVarA/V123], [$genVarB/V123], [$genVarC/V456]
     * (someRole: $genVarA, anotherRole: $genVarB, yetAnotherRole: $genVarC), [$genVarA/V456], [$genVarB/V123], [$genVarC/V456]
     * (someRole: $genVarA, anotherRole: $genVarB, yetAnotherRole: $genVarC), [$genVarA/V789], [$genVarB/V123], [$genVarC/V456]
     *
     * @param spec roleplayer configuration in the form {role} -> {type label, {ids...}}
     * @param relationIds list of id mappings for relation variable
     * @return list of generated patterns as strings
     */
    private static List<String> generateRelationPattern(
            Multimap<Label, Pair<Label, List<ConceptId>>> spec,
            List<ConceptId> relationIds){
        Var relationVar = !relationIds.isEmpty()? Graql.var().asUserDefined() : Graql.var();
        VarPattern[] basePattern = {relationVar};
        List<List<Pattern>> rpTypePatterns = new ArrayList<>();
        List<List<Pattern>> rpIdPatterns = new ArrayList<>();
        Multimap<Label, VarPattern> rps = HashMultimap.create();
        spec.entries().forEach(entry -> {
            VarPattern rolePlayer = Graql.var().asUserDefined();
            Label role = entry.getKey();
            Label type = entry.getValue().getKey();
            List<ConceptId> ids = entry.getValue().getValue();
            basePattern[0] = basePattern[0].rel(role.getValue(), rolePlayer);
            rps.put(role, rolePlayer);
            List<Pattern> rpPattern = Lists.newArrayList(rolePlayer);
            List<Pattern> typePattern = Lists.newArrayList(rolePlayer);
            if(type != null) typePattern.add(rolePlayer.isa(type.getValue()));

            ids.forEach(id -> {
                VarPattern idPattern = rolePlayer.id(id);
                //Pattern rolePlayerPattern = typePattern != null ? typePattern.and(idPattern) : idPattern;
                Pattern rolePlayerPattern = idPattern;
                rpPattern.add(rolePlayerPattern);
                relationIds.forEach(rid -> rpPattern.add(rolePlayerPattern.and(relationVar.id(rid))));
            });

            rpIdPatterns.add(rpPattern);
            rpTypePatterns.add(typePattern);
        });

        List<Pattern> patterns = new ArrayList<>();

        Stream.concat(
                Lists.cartesianProduct(rpTypePatterns).stream(),
                Lists.cartesianProduct(rpIdPatterns).stream()
        )
                //filter trivial patterns
                .map(l -> l.stream()
                        .filter(
                                p -> p.admin().isConjunction()
                                        || p.admin().asVarPattern().getProperties().findFirst().isPresent()
                        )
                        .collect(Collectors.toList()))
                .forEach(product -> {
            Pattern[] pattern = {basePattern[0]};
            product.forEach(p -> pattern[0] = pattern[0].and(p));
            if (!patterns.contains(pattern[0])) patterns.add(pattern[0]);
        });
        return patterns.stream().map(Object::toString).collect(Collectors.toList());
    }

    public final static TestQueryPattern differentRelationVariants = new TestQueryPattern() {
        @Override
        public List<String> patternList(Concept... args) {
            if (args.length != 3){ throw new IllegalStateException(); }
            ConceptId baseEntityId = args[0].id();
            ConceptId anotherBaseEntityId = args[1].id();
            ConceptId subEntityId = args[2].id();

            /**
            (baseRole1: $x, baseRole2: $y);,

             //1-3
             (baseRole1: $x1_2, baseRole2: $x2_2); $x2_2 isa anotherBaseRoleEntity;
             (baseRole1: $x1_1, baseRole2: $x2_1); $x1_1 isa baseRoleEntity;
             (baseRole1: $x1_3, baseRole2: $x2_3); $x1_3 isa baseRoleEntity; $x2_3 isa anotherBaseRoleEntity;

             //4-6
             "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x2_2b id '" + baseEntityId + "';}",
             "{(baseRole1: $x1_2c, baseRole2: $x2_2c); $x2_2c id '" + anotherBaseEntityId + "';}",
             "{(baseRole1: $x1_2d, baseRole2: $x2_2d); $x2_2d id '" + subEntityId + "';}",

             //7-10
             "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x1_1b id '" + baseEntityId + "';}",

             "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x1_1b id '" + baseEntityId + "'; $x2_2b id '" + baseEntityId + "';}",
             "{(baseRole1: $x1_2c, baseRole2: $x2_2c); $x1_1b id '" + baseEntityId + "'; $x2_2c id '" + anotherBaseEntityId + "';}",
             "{(baseRole1: $x1_2d, baseRole2: $x2_2d); $x1_1b id '" + baseEntityId + "'; $x2_2d id '" + subEntityId + "';}",

             //11-14
             "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x1_1b id '" + anotherBaseEntityId + "';}",

             "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x1_1b id '" + anotherBaseEntityId + "'; $x2_2b id '" + baseEntityId + "';}",
             "{(baseRole1: $x1_2c, baseRole2: $x2_2c); $x1_1b id '" + anotherBaseEntityId + "'; $x2_2c id '" + anotherBaseEntityId + "';}",
             "{(baseRole1: $x1_2d, baseRole2: $x2_2d); $x1_1b id '" + anotherBaseEntityId + "'; $x2_2d id '" + subEntityId + "';}",

             //15-18
             "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x1_1b id '" + subEntityId + "';}",

             "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x1_1b id '" + subEntityId + "'; $x2_2b id '" + baseEntityId + "';}",
             "{(baseRole1: $x1_2c, baseRole2: $x2_2c); $x1_1b id '" + subEntityId + "'; $x2_2c id '" + anotherBaseEntityId + "';}",
             "{(baseRole1: $x1_2d, baseRole2: $x2_2d); $x1_1b id '" + subEntityId + "'; $x2_2d id '" + subEntityId + "';}",

             //TODO
             //resources as roleplayers

             **/
            List<String> variants = generateRelationPattern(
                    ImmutableMultimap.of(
                            Label.of("baseRole1"), new Pair<>(Label.of("baseRoleEntity"), Lists.newArrayList(baseEntityId, anotherBaseEntityId, subEntityId)),
                            Label.of("baseRole2"), new Pair<>(Label.of("anotherBaseRoleEntity"), Lists.newArrayList(baseEntityId, anotherBaseEntityId, subEntityId))
                    ),
                    new ArrayList<>()
            );

            return variants;
        }
    };

    public final static TestQueryPattern differentRelationVariantsWithRelationVariable = new TestQueryPattern() {
        @Override
        public List<String> patternList(Concept... args) {
            if (args.length != 5){ throw new IllegalStateException(); }
            String baseEntityId = args[0].id().getValue();
            String anotherBaseEntityId = args[1].id().getValue();
            String subEntityId = args[2].id().getValue();
            String relationId = args[3].id().getValue();
            String anotherRelationId = args[4].id().getValue();
            return Lists.newArrayList(
                    "{$r ($x, $y);}",
                    "{$rb (role: $xb, role: $yb);}",
                    "{$rc (baseRole1: $xc, baseRole2: $yc);}",

                    //(x[], y), 3-6
                    "{$r1 (baseRole1: $x1_1, baseRole2: $x2_1); $x1_1 isa baseRoleEntity;}",
                    "{$r1b (baseRole1: $x1_1b, baseRole2: $x2_1b); $x1_1b id '" + baseEntityId + "';}",
                    "{$r1c (baseRole1: $x1_1c, baseRole2: $x2_1c); $x1_1c id '" + anotherBaseEntityId + "';}",
                    "{$r1e (baseRole1: $x1_1d, baseRole2: $x2_1d); $x1_1d id '" + subEntityId + "';}",

                    //(x, y[]), 7-10
                    "{$r2 (baseRole1: $x1_2, baseRole2: $x2_2); $x2_2 isa baseRoleEntity;}",
                    "{$r2b (baseRole1: $x1_2b, baseRole2: $x2_2b); $x2_2b id '" + baseEntityId + "';}",
                    "{$r2c (baseRole1: $x1_2c, baseRole2: $x2_2c); $x2_2c id '" + anotherBaseEntityId + "';}",
                    "{$r2d (baseRole1: $x1_2d, baseRole2: $x2_2d); $x2_2d id '" + subEntityId+ "';}",

                    //(x[], y[]), 11-13
                    "{$r5 (baseRole1: $x1_5, baseRole2: $x2_5); $x1_5 isa baseRoleEntity; $x2_5 isa anotherBaseRoleEntity;}",
                    "{$r5b (baseRole1: $x1_5b, baseRole2: $x2_5b); $x1_5b id '" + baseEntityId + "'; $x2_5b id '" + anotherBaseEntityId + "';}",
                    "{$r5c (baseRole1: $x1_5c, baseRole2: $x2_5c); $x1_5c id '" + subEntityId + "'; $x2_5c id '" + anotherBaseEntityId + "';}",

                    //14-18
                    "{$r6 (baseRole1: $x1_6, baseRole2: $x2_6); $r6 id '" + relationId + "';}",
                    "{$r6b (baseRole1: $x1_6b, baseRole2: $x2_6b); $r6b id '" + anotherRelationId + "';}",

                    "{$r6c (baseRole1: $x1_6c, baseRole2: $x2_6c); $x1_6c isa anotherBaseRoleEntity; $r6c id '" + relationId + "';}",
                    "{$r6d (baseRole1: $x1_6d, baseRole2: $x2_6d); $x2_6d id '" + baseEntityId + "';$r6d id '" + relationId + "';}",
                    "{$r6e (baseRole1: $x1_6e, baseRole2: $x2_6e); $x1_6e id '" + baseEntityId + "'; $x2_6e id '" + anotherBaseEntityId + "';$r6e id '" + relationId + "';}"

                    //TODO
                    //"{$r ($x);}"
                    //"{$rd (subRole1: $xd, subRole2: $yd);}",
            );

        }
    };

    public final static TestQueryPattern differentTypeVariants = new TestQueryPattern() {
        @Override
        public List<String> patternList(Concept... args) {
            if (args.length != 2){ throw new IllegalStateException(); }
            String resourceId = args[0].id().getValue();
            String anotherResourceId = args[1].id().getValue();
            return Lists.newArrayList(
                    "{$x isa $type;}",
                    "{$xb isa resource;}",
                    "{$xc isa! resource;}",
                    "{$xd isa $typed;$typed label resource-long;}",
                    "{$xe isa resource-long;}",
                    "{$xf isa attribute;}",

                    //6-8
                    "{$x1a isa resource; $x1a id '" + resourceId + "';}",
                    "{$x1b isa resource; $x1b id '" + anotherResourceId + "';}",
                    "{$x1c isa $type1; $type1 label resource;$x1c id '" + anotherResourceId + "';}",

                    //9-12
                    "{$x2a isa resource; $x2a == 'someValue';}",
                    "{$x2b isa resource; $x2b == 'someOtherValue';}",

                    "{$x2c isa resource; $x2c contains 'Value';}",
                    "{$x2d isa resource; $x2d contains 'Other';}",

                    //13-14
                    "{$x3a isa resource-long; $x3a == '0';}",
                    "{$x3b isa resource-long; $x3b == '1';}",

                    //15-18
                    "{$x4a isa resource-long; $x4a > '0';}",
                    "{$x4b isa resource-long; $x4b < '1';}",
                    "{$x4c isa resource-long; $x4c >= '0';}",
                    "{$x4d isa resource-long; $x4d <= '1';}"

                    //16-18
                    //TODO
                    //"{$x5a isa resource ; $x5a != $f;}",
                    //"{$x5b isa resource ; $x5b != $fb; $x5b id '" + resourceId + "';}",
                    //"{$x5c isa resource ; $x5c != $fc; $fc id '" + resourceId + "';}"
            );
        }
    };

    public final static TestQueryPattern differentResourceVariants = new TestQueryPattern() {
        @Override
        public List<String> patternList(Concept... args) {
            if (args.length != 4){ throw new IllegalStateException(); }
            String entityId = args[0].id().getValue();
            String anotherEntityId = args[1].id().getValue();
            String resourceId = args[2].id().getValue();
            String anotherResourceId = args[3].id().getValue();
            return Lists.newArrayList(
                    "{$x has resource $r;}",
                    "{$xb has resource-long $rb;}",
                    "{$xc has resource-long $rc; $xc isa baseRoleEntity;}",
                    "{$xd has attribute $rd;}",

                    //4-5
                    "{$x1a has resource $r1a; $x1a id '" + entityId + "';}",
                    "{$x1b has resource $r1b; $x1b id '" + anotherEntityId + "';}",

                    //6-7
                    "{$x2a has resource $r2a; $r2a id '" + resourceId + "';}",
                    "{$x2b has resource $r2b; $r2b id '" + anotherResourceId + "';}",

                    //8-13
                    "{$x3a has resource 'someValue';}",
                    "{$x3b has resource 'someOtherValue';}",
                    "{$x3c has resource $r3c; $r3c == 'someValue';}",
                    "{$x3d has resource $r3d; $r3d == 'someOtherValue';}",

                    "{$x3e has resource $r3e; $r3e contains 'Value';}",
                    "{$x3f has resource $r3f; $r3f contains 'Other';}",

                    //14-17
                    "{$x4a has resource-long 0;}",
                    "{$x4b has resource-long 1;}",
                    "{$x4c has resource-long $r4c; $r4c == 0;}",
                    "{$x4d has resource-long $r4d; $r4d == 1;}",

                    //18-21
                    "{$x5a has resource-long $r5a; $r5a > 0;}",
                    "{$x5b has resource-long $r5b; $r5b < 1;}",
                    "{$x5c has resource-long $r5c; $r5c >= 0;}",
                    "{$x5d has resource-long $r5d; $r5d <= 1;}",

                    //22-24
                    "{$x7a has resource-long $r7a;$r7a > 23; $r7a < 27;}",
                    "{$x7b isa baseRoleEntity, has resource-long $r7b;$r7b > 23; $r7b < 27;}",
                    "{$x7c isa $type;$type label baseRoleEntity;$x7c has resource-long $r7c;$r7c > 23; $r7c < 27;}",

                    //25-26
                    "{$x7d isa baseRoleEntity;$x7d has resource-long > 23;}",
                    "{$x7e isa baseRoleEntity;$x7e has resource-long $r7e;$r7e > 23;}",

                    //27-30
                    "{$x7f isa baseRoleEntity;$x7f has resource-long $r7f;$r7f > 27;$r7f < 29;}",
                    "{$x7g isa baseRoleEntity;$x7g has resource-long $r7g;$r7g > 27;$r7g < 23;}",
                    "{$x7h isa baseRoleEntity;$x7h has resource-long $r7h;$r7h > 23; $r2_7h < 27;}",
                    "{$x7i isa anotherBaseRoleEntity;$x7i has resource-long $r7i;$r7i > 23; $r7i < 27;}"

                    //TODO
                    //"{$xe has attribute $re;$re isa resource;}",

                    //TODO
                    //18-22
                    //"{$x6a has resource $r6a;$r6a != $r2_6a;}",
                    //"{$x6b has resource $r6b;$r6b != $r2_6b; $r2_6b id '" + resourceId + "';}",
                    //"{$x6c has resource $r6c;$x6c != $x2_6c;}",
                    //"{$x6d has resource $r6d;$x6d != $x2_6d; $x2_6d id '" + resourceId + "';}",
                    //"{$x6e has resource $r6e;$x6e != $r6e;}",
            );
        }
    };

    static <T> List<T> mergeToList(Collection<T>... lists){
        List<T> mergedList = new ArrayList<>();
        for (Collection<T> list : lists) mergedList.addAll(list);
        return mergedList;
    }

    static <T> List<T> subList(List<T> list, Collection<Integer> elements){
        List<T> subList = new ArrayList<>();
        elements.forEach(el -> subList.add(list.get(el)));
        return subList;
    }

    static <T> List<T> subListExcluding(List<T> list, Collection<Integer> toExclude){
        List<T> subList = new ArrayList<>(list);
        toExclude.forEach(el -> subList.remove(list.get(el)));
        return subList;
    }

    static <T> List<T> subListExcludingElements(List<T> list, Collection<T> toExclude){
        List<T> subList = new ArrayList<>(list);
        toExclude.forEach(subList::remove);
        return subList;
    }

}
