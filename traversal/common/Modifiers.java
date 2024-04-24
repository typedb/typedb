/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.common;

import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typeql.lang.common.TypeQLArg;
import com.vaticle.typeql.lang.common.TypeQLVariable;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.parameters.Order.Desc.DESC;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public class Modifiers {

    Filter filter;
    Sorting sorting;

    public Modifiers() {
        filter = new Filter(emptySet());
        sorting = new Sorting(emptyList(), map());
    }

    public Filter filter() {
        return filter;
    }

    public Modifiers filter(Filter filter) {
        this.filter = filter;
        return this;
    }

    public Sorting sorting() {
        return sorting;
    }

    public Modifiers sorting(Sorting sorting) {
        this.sorting = sorting;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Modifiers other = (Modifiers) o;
        return Objects.equals(filter, other.filter) && Objects.equals(sorting, other.sorting);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, sorting);
    }

    public static class Filter {

        private final Set<Identifier.Variable.Retrievable> variables;

        private Filter(Set<Identifier.Variable.Retrievable> variables) {
            this.variables = variables;
        }

        public static Filter create(Set<? extends Identifier.Variable.Retrievable> variables) {
            return new Filter(new HashSet<>(variables));
        }

        public static Filter create(List<? extends Identifier.Variable.Retrievable> variables) {
            return new Filter(new HashSet<>(variables));
        }

        public Set<Identifier.Variable.Retrievable> variables() {
            return variables;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Filter other = (Filter) o;
            return variables.equals(other.variables);
        }

        @Override
        public int hashCode() {
            return Objects.hash(variables);
        }
    }

    public static class Sorting {

        public static final Sorting EMPTY = new Sorting(list(), map());

        List<Identifier.Variable.Retrievable> variables;
        Map<Identifier.Variable.Retrievable, Order> ascending;

        private Sorting(List<Identifier.Variable.Retrievable> variables, Map<Identifier.Variable.Retrievable, Order> ascending) {
            assert variables.size() == ascending.size() && variables.containsAll(ascending.keySet());
            this.variables = variables;
            this.ascending = ascending;
        }

        public static Sorting create(List<Identifier.Variable.Retrievable> variables, Map<Identifier.Variable.Retrievable, Order> ascending) {
            return new Sorting(variables, ascending);
        }

        public static Sorting create(TypeQLQuery.Modifiers.Sorting sort) {
            List<Identifier.Variable.Retrievable> variables = new ArrayList<>();
            Map<Identifier.Variable.Retrievable, Order> ascending = new HashMap<>();
            sort.variables().forEach(typeQLVar -> {
                Identifier.Variable.Retrievable var = Identifier.Variable.of(typeQLVar.reference().asName());
                variables.add(var);
                ascending.put(var, sort.getOrder(typeQLVar) == TypeQLArg.Order.ASC ? ASC : DESC);
            });
            return new Sorting(variables, ascending);
        }

        public List<Identifier.Variable.Retrievable> variables() {
            return variables;
        }

        public Optional<Order> order(Identifier id) {
            if (!id.isRetrievable()) return Optional.empty();
            else return Optional.ofNullable(ascending.get(id.asVariable().asRetrievable()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Sorting other = (Sorting) o;
            return variables.equals(other.variables) && ascending.equals(other.ascending);
        }

        @Override
        public int hashCode() {
            return Objects.hash(variables, ascending);
        }
    }
}
