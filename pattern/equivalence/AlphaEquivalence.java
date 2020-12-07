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
 */

package grakn.core.pattern.equivalence;

import grakn.core.pattern.variable.Variable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Supplier;

public abstract class AlphaEquivalence {

    public static Valid valid() {
        return new Valid(new HashMap<>());
    }

    public static Invalid invalid() {
        return new Invalid();
    }

    public abstract AlphaEquivalence validIf(boolean invalidate);

    public abstract <T extends AlphaEquivalent<T>> AlphaEquivalence validIfAlphaEqual(T member1, T member2);

    public abstract <T extends AlphaEquivalent<T>> AlphaEquivalence validIfAlphaEqual(Set<T> set1, Set<T> set2);

    public abstract AlphaEquivalence addMapping(Variable from, Variable to);

    public abstract boolean isValid();

    public abstract Valid asValid();

    public abstract AlphaEquivalence addOrInvalidate(AlphaEquivalence mapping);

    public abstract AlphaEquivalence addOrInvalidate(Supplier<AlphaEquivalence> mappingSupplier);

    public static class Valid extends AlphaEquivalence {

        private final HashMap<Variable, Variable> map;

        private Valid(HashMap<Variable, Variable> map) {
            this.map = map;
        }

        public static AlphaEquivalence.Valid create() {
            return new AlphaEquivalence.Valid(new HashMap<>());
        }

        public static AlphaEquivalence.Valid create(HashMap<Variable, Variable> map) {
            return new AlphaEquivalence.Valid(map);
        }

        @Override
        public AlphaEquivalence addOrInvalidate(AlphaEquivalence alphaMap) {
            if (!alphaMap.isValid()) return AlphaEquivalence.invalid();
            map.putAll(alphaMap.asValid().map());
            return this;
        }

        @Override
        public AlphaEquivalence addOrInvalidate(Supplier<AlphaEquivalence> mappingSupplier) {
            return addOrInvalidate(mappingSupplier.get());
        }

        @Override
        public <T extends AlphaEquivalent<T>> AlphaEquivalence validIfAlphaEqual(Set<T> set1, Set<T> set2) {
            return addOrInvalidate(EquivalenceSet.of(set1).alphaEquals(EquivalenceSet.of(set2)));
        }

        @Override
        public <T extends AlphaEquivalent<T>> AlphaEquivalence validIfAlphaEqual(@Nullable T member1, @Nullable T member2) {
            if (member1 == null && member2 == null) return Valid.create();
            if (member1 != null && member2 != null) return addOrInvalidate(member1.alphaEquals(member2));
            return AlphaEquivalence.invalid();
        }

        @Override
        public Valid addMapping(Variable from, Variable to) {
            map.put(from, to);
            return this;
        }

        @Override
        public AlphaEquivalence validIf(boolean valid) {
            if (!valid) return AlphaEquivalence.invalid();
            return this;
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public Valid asValid() {
            return this;
        }

        public HashMap<Variable, Variable> map() {
            return new HashMap<>(map);
        }
    }

    public static class Invalid extends AlphaEquivalence {

        @Override
        public Invalid addOrInvalidate(AlphaEquivalence mapping) {
            return this;
        }

        @Override
        public AlphaEquivalence addOrInvalidate(Supplier<AlphaEquivalence> mappingSupplier) {
            return this;
        }

        @Override
        public <T extends AlphaEquivalent<T>> Invalid validIfAlphaEqual(T member1, T member2) {
            return this;
        }

        @Override
        public Invalid addMapping(Variable from, Variable to) {
            return this;
        }

        @Override
        public Invalid validIf(boolean invalidate) {
            return this;
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public Valid asValid() {
            throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Valid.class.getSimpleName());
        }

        @Override
        public <T extends AlphaEquivalent<T>> AlphaEquivalence validIfAlphaEqual(Set<T> set1, Set<T> set2) {
            return this;
        }
    }

    public static class EquivalenceSet<T extends AlphaEquivalent<T>> implements AlphaEquivalent<EquivalenceSet<T>> {

        private final Set<T> set;

        private EquivalenceSet(Set<T> set) {
            this.set = set;
        }

        public static <S extends AlphaEquivalent<S>> EquivalenceSet<S> of(Set<S> set) {
            return new EquivalenceSet<>(set);
        }

        @Override
        public AlphaEquivalence alphaEquals(EquivalenceSet<T> that) { // TODO Should be able to accept a set not an alpha set?
            if (that.size() != size())
                return AlphaEquivalence.invalid();
            try {
                return containsAll(that);
            } catch (NullPointerException unused) {
                return AlphaEquivalence.invalid();
            }
        }

        private int size() {
            return set.size();
        }

        private Iterator<T> iterator() {
            return set.iterator();
        }

        private AlphaEquivalence containsAll(EquivalenceSet<T> c) {
            AlphaEquivalence alphaMap = Valid.create();
            for (T e : c.set())
                alphaMap = alphaMap.addOrInvalidate(contains(e));
            if (!alphaMap.isValid())
                return alphaMap;
            return alphaMap;
        }

        private AlphaEquivalence contains(T o) {
            Iterator<T> it = iterator();
            if (o == null) {
                while (it.hasNext())
                    if (it.next() == null)
                        return Valid.create();
            } else {
                while (it.hasNext()) {
                    AlphaEquivalence alphaMap = it.next().alphaEquals(o);
                    if (alphaMap.isValid())
                        return Valid.create(alphaMap.asValid().map());
                }
            }
            return AlphaEquivalence.invalid();
        }

        private Set<T> set() {
            return set;
        }

    }
}
