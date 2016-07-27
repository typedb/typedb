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

package io.mindmaps.graql.api.query;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * A pattern describing a subgraph.
 * <p>
 * A {@code Pattern} can describe an entire graph, or just a single concept.
 * <p>
 * For example, {@code var("x").isa("movie")} is a pattern representing things that are movies.
 * <p>
 * A pattern can also be a conjunction: {@code and(var("x").isa("movie"), var("x").value("Titanic"))}, or a disjunction:
 * {@code or(var("x").isa("movie"), var("x").isa("tv-show"))}. These can be used to combine other patterns together
 * into larger patterns.
 */
public interface Pattern {

    /**
     * @return an Admin class that allows inspecting or manipulating this pattern
     */
    Admin admin();

    /**
     * Admin class for inspecting and manipulating a Pattern
     */
    interface Admin extends Pattern {
        /**
         * Get the disjunctive normal form of this pattern group.
         * This means the pattern group will be transformed into a number of conjunctive patterns, where each is disjunct.
         *
         * e.g.
         * p = (A or B) and (C or D)
         * p.getDisjunctiveNormalForm() = (A and C) or (A and D) or (B and C) or (B and D)
         *
         * @return the pattern group in disjunctive normal form
         */
        Disjunction<Conjunction<Var.Admin>> getDisjunctiveNormalForm();

        /**
         * @return true if this Pattern.Admin is a Conjunction
         */
        default boolean isDisjunction() {
            return false;
        }

        /**
         * @return true if this Pattern.Admin is a Disjunction
         */
        default boolean isConjunction() {
            return false;
        }

        /**
         * @return true if this Pattern.Admin is a Var.Admin
         */
        default boolean isVar() {
            return false;
        }

        /**
         * @return this Pattern.Admin as a Disjunction, if it is one.
         */
        default Disjunction<?> asDisjunction() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return this Pattern.Admin as a Conjunction, if it is one.
         */
        default Conjunction<?> asConjunction() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return this Pattern.Admin as a Var.Admin, if it is one.
         */
        default Var.Admin asVar() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return all variables referenced in the pattern
         */
        default Set<Var.Admin> getVars() {
            return getDisjunctiveNormalForm().getPatterns().stream()
                    .flatMap(conj -> conj.getPatterns().stream())
                    .collect(toSet());
        }

        /**
         * @param patterns the patterns to join into a conjunction
         * @return a conjunction of the given patterns
         */
        static <T extends Admin> Conjunction<T> conjunction(Set<T> patterns) {
            return new Conjunction<>(Objects.requireNonNull(patterns));
        }

        /**
         * @param patterns the patterns to join into a disjunction
         * @return a disjunction of the given patterns
         */
        static <T extends Admin> Disjunction<T> disjunction(Set<T> patterns) {
            return new Disjunction<>(Objects.requireNonNull(patterns));
        }
    }

    /**
     * A class representing a conjunction (and) of patterns. All inner patterns must match in a query
     */
    class Conjunction<T extends Admin> implements Admin {

        private final Set<T> patterns;

        private Conjunction(Set<T> patterns) {
            this.patterns = patterns;
        }

        /**
         * @return the patterns within this conjunction
         */
        public Set<T> getPatterns() {
            return patterns;
        }

        @Override
        public Disjunction<Conjunction<Var.Admin>> getDisjunctiveNormalForm() {
            // Get all disjunctions in query
            List<Set<Conjunction<Var.Admin>>> disjunctionsOfConjunctions = patterns.stream()
                    .map(p -> p.getDisjunctiveNormalForm().getPatterns())
                    .collect(toList());

            // Get the cartesian product.
            // in other words, this puts the 'ands' on the inside and the 'ors' on the outside
            // e.g. (A or B) and (C or D)  <=>  (A and C) or (A and D) or (B and C) or (B and D)
            Set<Conjunction<Var.Admin>> dnf = Sets.cartesianProduct(disjunctionsOfConjunctions).stream()
                    .map(Conjunction::fromConjunctions)
                    .collect(toSet());

            return Admin.disjunction(dnf);

            // Wasn't that a horrible function? Here it is in Haskell:
            //     dnf = map fromConjunctions . sequence . map getDisjunctiveNormalForm . patterns
        }

        @Override
        public boolean isConjunction() {
            return true;
        }

        @Override
        public Conjunction<?> asConjunction() {
            return this;
        }

        private static <U extends Admin> Conjunction<U> fromConjunctions(List<Conjunction<U>> conjunctions) {
            Set<U> patterns = conjunctions.stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
            return new Conjunction<>(patterns);
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof Conjunction) && patterns.equals(((Conjunction) obj).patterns);
        }

        @Override
        public int hashCode() {
            return patterns.hashCode();
        }

        @Override
        public String toString() {
            return patterns.stream().map(Object::toString).collect(Collectors.joining("; "));
        }

        @Override
        public Admin admin() {
            return this;
        }
    }

    /**
     * A class representing a disjunction (or) of patterns. Any inner pattern must match in a query
     */
    class Disjunction<T extends Admin> implements Admin {

        private final Set<T> patterns;

        private Disjunction(Set<T> patterns) {
            this.patterns = patterns;
        }

        /**
         * @return the patterns within this disjunction
         */
        public Set<T> getPatterns() {
            return patterns;
        }

        @Override
        public Disjunction<Conjunction<Var.Admin>> getDisjunctiveNormalForm() {
            // Concatenate all disjunctions into one big disjunction
            Set<Conjunction<Var.Admin>> dnf = patterns.stream()
                    .flatMap(p -> p.getDisjunctiveNormalForm().getPatterns().stream())
                    .collect(toSet());

            return Admin.disjunction(dnf);
        }

        @Override
        public boolean isDisjunction() {
            return true;
        }

        @Override
        public Disjunction<?> asDisjunction() {
            return this;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof Disjunction) && patterns.equals(((Disjunction) obj).patterns);
        }

        @Override
        public int hashCode() {
            return patterns.hashCode();
        }

        @Override
        public String toString() {
            return patterns.stream().map(p -> "{" + p.toString() + "}").collect(Collectors.joining(" or "));
        }

        @Override
        public Admin admin() {
            return this;
        }
    }
}
