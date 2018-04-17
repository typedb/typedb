/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Implementation of the {@link MultiUnifier} interface.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class MultiUnifierImpl implements MultiUnifier{

    private final ImmutableSet<Unifier> multiUnifier;

    public MultiUnifierImpl(Set<Unifier> us){
        this.multiUnifier = ImmutableSet.copyOf(us);
    }
    public MultiUnifierImpl(Unifier u){
        this.multiUnifier = ImmutableSet.of(u);
    }

    /**
     * identity multiunifier
     */
    public MultiUnifierImpl(){
        this.multiUnifier = ImmutableSet.of(new UnifierImpl());
    }

    @SafeVarargs
    MultiUnifierImpl(ImmutableMultimap<Var, Var>... maps){
        this.multiUnifier = ImmutableSet.<Unifier>builder()
                .addAll(Stream.of(maps).map(UnifierImpl::new).iterator())
                .build();
    }

    @Override
    public Stream<Unifier> stream() {
        return multiUnifier.stream();
    }

    @Override
    public Iterator<Unifier> iterator() {
        return multiUnifier.iterator();
    }

    @Override
    public Unifier getUnifier() {
        return Iterables.getOnlyElement(multiUnifier);
    }

    @Override
    public Unifier getAny() {
        //TODO add a check it's a structural one
        UnmodifiableIterator<Unifier> iterator = multiUnifier.iterator();
        if (!iterator.hasNext()){
            throw GraqlQueryException.nonExistentUnifier();
        }
        return iterator.next();
    }

    @Override
    public ImmutableSet<Unifier> unifiers() { return multiUnifier;}

    @Override
    public boolean isEmpty() {
        return multiUnifier.isEmpty();
    }

    public boolean contains(Unifier u2) {
        return unifiers().stream().anyMatch(u -> u.containsAll(u2));
    }

    @Override
    public boolean containsAll(MultiUnifier mu) {
        return mu.unifiers().stream()
                .allMatch(this::contains);
    }

    @Override
    public MultiUnifier inverse() {
        return new MultiUnifierImpl(
                multiUnifier.stream()
                        .map(Unifier::inverse)
                        .collect(Collectors.toSet())
        );
    }

    @Override
    public int size() {
        return multiUnifier.size();
    }
}
