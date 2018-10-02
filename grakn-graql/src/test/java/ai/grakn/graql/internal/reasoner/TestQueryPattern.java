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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class TestQueryPattern {

    public abstract List<String> patternList(Concept... args);

    public final static TestQueryPattern differentRelationVariants = new TestQueryPattern() {
        @Override
        public List<String> patternList(Concept... args) {
            if (args.length != 3){ throw new IllegalStateException(); }
            String baseEntityId = args[0].id().getValue();
            String anotherBaseEntityId = args[1].id().getValue();
            String subEntityId = args[2].id().getValue();
            return Lists.newArrayList(
                    "{(baseRole1: $x, baseRole2: $y);}",

                    //(x[], y), 1-4
                    "{(baseRole1: $x1_1, baseRole2: $x2_1); $x1_1 isa baseRoleEntity;}",
                    "{(baseRole1: $x1_1b, baseRole2: $x2_1b); $x1_1b id '" + baseEntityId + "';}",
                    "{(baseRole1: $x1_1c, baseRole2: $x2_1c); $x1_1c id '" + anotherBaseEntityId + "';}",
                    "{(baseRole1: $x1_1d, baseRole2: $x2_1d); $x1_1d id '" + subEntityId + "';}",

                    //(x, y[]), 5-8
                    "{(baseRole1: $x1_2, baseRole2: $x2_2); $x2_2 isa baseRoleEntity;}",
                    "{(baseRole1: $x1_2b, baseRole2: $x2_2b); $x2_2b id '" + baseEntityId + "';}",
                    "{(baseRole1: $x1_2c, baseRole2: $x2_2c); $x2_2c id '" + anotherBaseEntityId + "';}",
                    "{(baseRole1: $x1_2d, baseRole2: $x2_2d); $x2_2d id '" + subEntityId + "';}",

                    //(x[], y[]), 9-11
                    "{(baseRole1: $x1_3, baseRole2: $x2_3); $x1_3 isa baseRoleEntity; $x2_3 isa anotherBaseRoleEntity;}",
                    "{(baseRole1: $x1_3b, baseRole2: $x2_3b); $x1_3b id '" + baseEntityId + "'; $x2_3b id '" + anotherBaseEntityId + "';}",
                    "{(baseRole1: $x1_3c, baseRole2: $x2_3c); $x1_3c id '" + subEntityId + "'; $x2_3c id '" + anotherBaseEntityId + "';}"
                );
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
                    "{$r (baseRole1: $x, baseRole2: $y);}",

                    //(x[], y), 1-4
                    "{$r1 (baseRole1: $x1_1, baseRole2: $x2_1); $x1_1 isa baseRoleEntity;}",
                    "{$r1b (baseRole1: $x1_1b, baseRole2: $x2_1b); $x1_1b id '" + baseEntityId + "';}",
                    "{$r1c (baseRole1: $x1_1c, baseRole2: $x2_1c); $x1_1c id '" + anotherBaseEntityId + "';}",
                    "{$r1e (baseRole1: $x1_1d, baseRole2: $x2_1d); $x1_1d id '" + subEntityId + "';}",

                    //(x, y[]), 5-8
                    "{$r2 (baseRole1: $x1_2, baseRole2: $x2_2); $x2_2 isa baseRoleEntity;}",
                    "{$r2b (baseRole1: $x1_2b, baseRole2: $x2_2b); $x2_2b id '" + baseEntityId + "';}",
                    "{$r2c (baseRole1: $x1_2c, baseRole2: $x2_2c); $x2_2c id '" + anotherBaseEntityId + "';}",
                    "{$r2d (baseRole1: $x1_2d, baseRole2: $x2_2d); $x2_2d id '" + subEntityId+ "';}",

                    //(x[], y[]), 9-11
                    "{$r5 (baseRole1: $x1_5, baseRole2: $x2_5); $x1_5 isa baseRoleEntity; $x2_5 isa anotherBaseRoleEntity;}",
                    "{$r5b (baseRole1: $x1_5b, baseRole2: $x2_5b); $x1_5b id '" + baseEntityId + "'; $x2_5b id '" + anotherBaseEntityId + "';}",
                    "{$r5c (baseRole1: $x1_5c, baseRole2: $x2_5c); $x1_5c id '" + subEntityId + "'; $x2_5c id '" + anotherBaseEntityId + "';}",

                    //12-16
                    "{$r6 (baseRole1: $x1_6, baseRole2: $x2_6); $r6 id '" + relationId + "';}",
                    "{$r6b (baseRole1: $x1_6b, baseRole2: $x2_6b); $r6b id '" + anotherRelationId + "';}",
                    "{$r6c (baseRole1: $x1_6c, baseRole2: $x2_6c); $x1_6c isa anotherBaseRoleEntity; $r6c id '" + relationId + "';}",
                    "{$r6d (baseRole1: $x1_6d, baseRole2: $x2_6d); $x2_6d id '" + baseEntityId + "';$r6d id '" + relationId + "';}",
                    "{$r6e (baseRole1: $x1_6e, baseRole2: $x2_6e); $x1_6e id '" + baseEntityId + "'; $x2_6e id '" + anotherBaseEntityId + "';$r6e id '" + relationId + "';}"
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
                    "{$x isa resource;}",
                    "{$xb isa resource-long;}",

                    //2-3
                    "{$x1a isa resource; $x1a id '" + resourceId + "';}",
                    "{$x1b isa resource; $x1b id '" + anotherResourceId + "';}",

                    //4-5
                    "{$x2a isa resource; $x2a == 'someValue';}",
                    "{$x2b isa resource; $x2b == 'someOtherValue';}",

                    //6-7
                    "{$x3a isa resource-long; $x3a == '0';}",
                    "{$x3b isa resource-long; $x3b == '1';}",

                    //7-8
                    "{$x4a isa resource-long; $x4a > '0';}",
                    "{$x4b isa resource-long; $x4b < '1';}",
                    "{$x4c isa resource-long; $x4c >= '0';}",
                    "{$x4d isa resource-long; $x4d <= '1';}"
            );
        }
    };

    public final static TestQueryPattern differentResourceVariants = new TestQueryPattern() {
        @Override
        public List<String> patternList(Concept... args) {
            if (args.length != 4){ throw new IllegalStateException(); }
            String entityId = args[0].id().getValue();
            String anotherEntityId = args[1].id().getValue();
            String resourceId = args[0].id().getValue();
            String anotherResourceId = args[0].id().getValue();
            return Lists.newArrayList(
                    "{$x has resource $r;}",
                    "{$xb has resource-long $rb;}",

                    //2-3
                    "{$x1a has resource $r1a; $x1a id '" + entityId + "';}",
                    "{$x1b has resource $r1b; $x1b id '" + anotherEntityId + "';}",

                    //4-5
                    "{$x2a has resource $r2a; $r2a id '" + resourceId + "';}",
                    "{$x2b has resource $r2b; $r2b id '" + anotherResourceId + "';}",

                    //6-9
                    "{$x3a has resource 'someValue';}",
                    "{$x3b has resource 'someOtherValue';}",
                    "{$x3c has resource $x3c; $x3c == 'someValue';}",
                    "{$x3d has resource $x3d; $x3d == 'someOtherValue';}",

                    //10-13
                    "{$x4a has resource-long '0';}",
                    "{$x4b has resource-long '1';}",
                    "{$x4c has resource-long $x4c; $x4c == '0';}",
                    "{$x4d has resource-long $x4d; $x4d == '1';}",

                    //14-17
                    "{$x5a has resource-long $x5a; $x5a > '0';}",
                    "{$x5b has resource-long $x5b; $x5b < '1';}",
                    "{$x5c has resource-long $x5c; $x5c >= '0';}",
                    "{$x5d has resource-long $x5d; $x5d <= '1';}"
            );
        }
    };

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

    void orthogonalityTest(){

    }




}
