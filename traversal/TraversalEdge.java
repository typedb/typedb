/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.traversal;

public abstract class TraversalEdge {

//    public static class Is extends TraversalEdge {
//
//        private final Reference concept1, concept2;
//
//        public Is(final Reference concept1, final Reference concept2) {
//            this.concept1 = concept1;
//            this.concept2 = concept2;
//            directedTraversals.add(new Is.Out());
//            directedTraversals.add(new Is.In());
//        }
//
//        public static Is of(final Reference concept1, final Reference concept2) {
//            return new Is(concept1, concept2);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class Sub extends TraversalEdge {
//
//        private final Reference thingType, superType;
//
//        public Sub(final Reference thingType, final Reference superType) {
//            this.thingType = thingType;
//            this.superType = superType;
//            directedTraversals.add(new Sub.Out());
//            directedTraversals.add(new Sub.In());
//        }
//
//        public static Sub of(final Reference thingType, final Reference superType) {
//            return new Sub(thingType, superType);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class Owns extends TraversalEdge {
//
//        private final Reference thingType, attributeType;
//
//        public Owns(final Reference thingType, final Reference attributeType) {
//            this.thingType = thingType;
//            this.attributeType = attributeType;
//            directedTraversals.add(new Owns.Out());
//            directedTraversals.add(new Owns.In());
//        }
//
//        public static Owns of(final Reference thingType, final Reference attributeType) {
//            return new Owns(thingType, attributeType);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class Plays extends TraversalEdge {
//
//        private final Reference thing, role;
//
//        public Plays(final Reference thing, final Reference role) {
//            this.thing = thing;
//            this.role = role;
//            directedTraversals.add(new Plays.Out());
//            directedTraversals.add(new Plays.In());
//        }
//
//        public static Plays of(final Reference thingType, final Reference roleType) {
//            return new Plays(thingType, roleType);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class Relates extends TraversalEdge {
//
//        private final Reference relationType, roleType;
//
//        public Relates(final Reference relationType, final Reference roleType) {
//            this.relationType = relationType;
//            this.roleType = roleType;
//            directedTraversals.add(new Relates.Out());
//            directedTraversals.add(new Relates.In());
//        }
//
//        public static Relates of(final Reference relationType, final Reference roleType) {
//            return new Relates(relationType, roleType);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class Isa extends TraversalEdge {
//
//        private final Reference thing, type;
//        private final boolean isExplicit;
//
//        public Isa(final Reference thing, final Reference type, final boolean isExplicit) {
//            this.thing = thing;
//            this.type = type;
//            this.isExplicit = isExplicit;
//            directedTraversals.add(new Isa.Out());
//            directedTraversals.add(new Isa.In());
//        }
//
//        public static Isa of(final Reference thing, final Reference type, final boolean isExplicit) {
//            return new Isa(thing, type, isExplicit);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class Has extends TraversalEdge {
//
//        private final Reference owner, attribute;
//
//        Has(final Reference owner, final Reference attribute) {
//            this.owner = owner;
//            this.attribute = attribute;
//            directedTraversals.add(new Has.Out());
//            directedTraversals.add(new Has.In());
//        }
//
//        public static Has of(final Reference owner, final Reference attribute) {
//            return new Has(owner, attribute);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class Playing extends TraversalEdge {
//
//        private final Reference thing, role;
//
//        public Playing(final Reference thing, final Reference role) {
//            this.thing = thing;
//            this.role = role;
//            directedTraversals.add(new Playing.Out());
//            directedTraversals.add(new Playing.In());
//        }
//
//        public static Playing of(final Reference thing, final Reference role) {
//            return new Playing(thing, role);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class Relating extends TraversalEdge {
//
//        private final Reference relation, role;
//
//        public Relating(final Reference relation, final Reference role) {
//            this.relation = relation;
//            this.role = role;
//            directedTraversals.add(new Relating.Out());
//            directedTraversals.add(new Relating.In());
//        }
//
//        public static Relating of(final Reference relation, final Reference role) {
//            return new Relating(relation, role);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
//
//    public static class RolePlayer extends TraversalEdge {
//
//        private final Reference relation, roleType, player;
//
//        public RolePlayer(final Reference relation, @Nullable final Reference roleType, final Reference player) {
//            this.roleType = roleType;
//            this.relation = relation;
//            this.player = player;
//            directedTraversals.add(new RolePlayer.Out());
//            directedTraversals.add(new RolePlayer.In());
//        }
//
//        public static RolePlayer of(final Reference relation, final Reference player) {
//            return of(relation, null, player);
//        }
//
//        public static RolePlayer of(final Reference relation, @Nullable final Reference roleType,
//                                    final Reference player) {
//            return new RolePlayer(relation, roleType, player);
//        }
//
//        private class Out extends Directed {}
//
//        private class In extends Directed {}
//    }
}
