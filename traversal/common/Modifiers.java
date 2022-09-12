package com.vaticle.typedb.core.traversal.common;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typeql.lang.common.TypeQLArg;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.builder.Sortable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Modifiers {

    Filter filter;
    Sorting sorting;

    public Modifiers() {
    }

    public Filter filter() {
        return filter;
    }

    public void filter(Filter filter) {
        this.filter = filter;
    }

    public Sorting sorting() {
        return sorting;
    }

    public void sorting(Sorting sorting) {
        this.sorting = sorting;
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

        Set<Identifier.Variable.Retrievable> variables;

        private Filter(Set<Identifier.Variable.Retrievable> variables) {
            this.variables = variables;
        }

        public static Filter create(Set<Identifier.Variable.Retrievable> variables) {
            return new Filter(variables);
        }

        public static Filter create(List<UnboundVariable> vars) {
            Set<Identifier.Variable.Retrievable> variables = new HashSet<>();
            iterate(vars).map(v -> Identifier.Variable.of(v.reference().asName())).forEachRemaining(variables::add);
            return new Filter(variables);
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

        List<Identifier.Variable.Retrievable> variables;
        Map<Identifier.Variable.Retrievable, Boolean> ascending;

        private Sorting(List<Identifier.Variable.Retrievable> variables, Map<Identifier.Variable.Retrievable, Boolean> ascending) {
            assert variables.size() == ascending.size() && variables.containsAll(ascending.keySet());
            this.variables = variables;
            this.ascending = ascending;
        }

        public static Sorting create(List<Pair<Identifier.Variable.Retrievable, Boolean>> sorting) {
            List<Identifier.Variable.Retrievable> variables = new ArrayList<>();
            Map<Identifier.Variable.Retrievable, Boolean> ascending = new HashMap<>();
            sorting.forEach(pair -> {
                variables.add(pair.first());
                ascending.put(pair.first(), pair.second());
            });
            return new Sorting(variables, ascending);
        }

        public static Sorting create(Sortable.Sorting sort) {
            List<Identifier.Variable.Retrievable> variables = new ArrayList<>();
            Map<Identifier.Variable.Retrievable, Boolean> ascending = new HashMap<>();
            // TODO: sort order per-variable
            sort.vars().forEach(typeQLVar -> {
                Identifier.Variable.Retrievable var = Identifier.Variable.of(typeQLVar.reference().asReferable()).asRetrievable();
                variables.add(var);
                ascending.put(var, sort.order() == TypeQLArg.Order.ASC);
            });
            return new Sorting(variables, ascending);
        }

        public List<Identifier.Variable.Retrievable> variables() {
            return variables;
        }

        public boolean isAscending(Identifier.Variable.Retrievable var) {
            assert ascending.containsKey(var);
            return ascending.get(var);
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
