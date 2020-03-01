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

package grakn.core.graql.reasoner.cache;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Query answer set indexed with partial substitutions (partial answers).
 */
public class IndexedAnswerSet implements AnswerSet{

    private final HashMultimap<ConceptMap, ConceptMap> indexedAnswers = HashMultimap.create();

    //indices are really var sets
    private final Index index;

    private IndexedAnswerSet(Index index){
        this.index = index;
    }

    public static IndexedAnswerSet create(Index index){
        return new IndexedAnswerSet(index);
    }

    public static IndexedAnswerSet create(ConceptMap answer, Index index){
        return create(Sets.newHashSet(answer), index);
    }

    public static IndexedAnswerSet create(Set<ConceptMap> answers, Index index){
        IndexedAnswerSet answerSet = new IndexedAnswerSet(index);
        answerSet.addAll(answers);
        return answerSet;
    }

    @Override
    public int size() { return getAll().size();}

    @Override
    public boolean contains(Object o) { return getAll().contains(o); }

    @Override
    public Object[] toArray() { return getAll().toArray(); }

    @Override
    public <T> T[] toArray(T[] ts) { return getAll().toArray(ts); }

    @Override
    public boolean remove(Object o) {
        return indexedAnswers.asMap().entrySet().stream()
                .filter(e -> e.getValue().contains(o))
                .allMatch(e -> e.getValue().remove(o));
    }

    @Override
    public boolean containsAll(Collection<?> collection) { return getAll().containsAll(collection); }

    @Override
    public boolean addAll(Collection<? extends ConceptMap> collection) { return collection.stream().allMatch(this::add); }

    @Override
    public boolean retainAll(Collection<?> collection) { return false; }

    @Override
    public boolean removeAll(Collection<?> collection) { return collection.stream().allMatch(this::remove); }

    @Override
    public void clear() { indexedAnswers.clear(); }

    public Multiset<ConceptMap> keys(){ return indexedAnswers.keys();}

    @Override
    public Set<ConceptMap> get(ConceptMap sub) {
        Index ind = Index.of(sub.vars());
        if (ind.equals(index)){
            return indexedAnswers.get(sub);
        }
        throw new IllegalStateException("Illegal index: " + sub + " indices: " + index);
    }

    @Override
    public Set<ConceptMap> getAll() {
        if (index.equals(Index.empty())) return indexedAnswers.get(new ConceptMap());
        return new HashSet<>(indexedAnswers.values());
    }

    @Override
    //add answer to all indices
    public boolean add(ConceptMap answer) {
        return add(answer, answer.project(index.vars()));
    }

    //add answer with specific index
    public boolean add(ConceptMap answer, ConceptMap answerIndex){
        Index ind = Index.of(answerIndex.vars());
        if (ind.equals(index)) {
            return indexedAnswers.put(answerIndex, answer);
        }
        throw new IllegalStateException("Illegal index: " + answerIndex + " indices: " + index);
    }

    @Override
    public Stream<ConceptMap> stream() {
        return getAll().stream();
    }

    @Override
    public Iterator<ConceptMap> iterator() {
        return getAll().iterator();
    }

    @Override
    public boolean isEmpty() {
        return indexedAnswers.isEmpty();
    }
}
