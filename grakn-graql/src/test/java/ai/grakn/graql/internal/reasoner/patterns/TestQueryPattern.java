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

package ai.grakn.graql.internal.reasoner.patterns;

import ai.grakn.concept.ConceptId;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class TestQueryPattern {

    public abstract List<String> patterns(ConceptId id, ConceptId anotherId, ConceptId extraId, ConceptId anotherExtraId);

    public final static TestQueryPattern differentTypeVariants = new TestQueryPattern() {

        @Override
        public List<String> patterns(ConceptId resourceId, ConceptId anotherResourceId, ConceptId extraId, ConceptId anotherExtraId) {
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
        public List<String> patterns(ConceptId entityId, ConceptId anotherEntityId, ConceptId resourceId, ConceptId anotherResourceId) {
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

    static int[][] identity(int N){
        int[][] matrix = new int[N][N];
        for(int i = 0; i < N ; i++)
            for(int j = 0; j < N ; j++){
                if (i == j) matrix[i][j] = 1;
                else matrix[i][j] = 0;
            }
        return matrix;
    }

    public static <T> List<T> subList(List<T> list, Collection<Integer> elements){
        List<T> subList = new ArrayList<>();
        elements.forEach(el -> subList.add(list.get(el)));
        return subList;
    }

    public static <T> List<T> subListExcluding(List<T> list, Collection<Integer> toExclude){
        List<T> subList = new ArrayList<>(list);
        toExclude.forEach(el -> subList.remove(list.get(el)));
        return subList;
    }

    public static <T> List<T> subListExcludingElements(List<T> list, Collection<T> toExclude){
        List<T> subList = new ArrayList<>(list);
        toExclude.forEach(subList::remove);
        return subList;
    }

}
