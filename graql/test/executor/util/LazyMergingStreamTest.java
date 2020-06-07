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

package grakn.core.graql.executor.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LazyMergingStreamTest {

    @Test
    public void whenOuterStreamEmptyNoErrorThrown() {
        Stream<Stream<Integer>> emptyStream = Stream.empty();
        LazyMergingStream<Integer> mergingStream = new LazyMergingStream<>(emptyStream);
        long count = mergingStream.flatStream().count();
        assertEquals(0, count);
    }

    @Test
    public void whenInnerStreamEmptyNoErrorIsThrown() {
        Stream<Stream<Integer>> emptyStream = Stream.of(Stream.empty());
        LazyMergingStream<Integer> mergingStream = new LazyMergingStream<>(emptyStream);
        long count = mergingStream.flatStream().count();
        assertEquals(0, count);
    }

    @Test
    public void whenAllInnerStreamsEmptyNoErrorIsThrown() {
        Stream<Stream<Integer>> emptyStream = Stream.of(Stream.empty(), Stream.empty(), Stream.empty());
        LazyMergingStream<Integer> mergingStream = new LazyMergingStream<>(emptyStream);
        long count = mergingStream.flatStream().count();
        assertEquals(0, count);
    }

    @Test
    public void whenoneInnerStreamsEmptyNoErrorIsThrown() {
        Stream<Stream<Integer>> emptyStream = Stream.of(Stream.of(1,2), Stream.empty(), Stream.of(3,4));
        LazyMergingStream<Integer> mergingStream = new LazyMergingStream<>(emptyStream);
        long count = mergingStream.flatStream().count();
        assertEquals(4, count);
    }

    @Test
    public void outerStreamIsConsumedLazily() {
        Stream<Stream<Integer>> mockStream = mock(Stream.class);
        Stream<Integer> mockFirstInnerStream = mock(Stream.class);
        Stream<Integer> mockSecondInnerStream = mock(Stream.class);

        Iterator<Stream<Integer>> mockIterator = mock(Iterator.class);
        when(mockStream.iterator()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockIterator.next()).thenReturn(mockFirstInnerStream).thenReturn(mockSecondInnerStream);

        when(mockFirstInnerStream.iterator()).thenReturn(Arrays.asList(1).iterator());
        when(mockSecondInnerStream.iterator()).thenReturn(Arrays.asList(21).iterator());

        LazyMergingStream<Integer> mergingStream = new LazyMergingStream<>(mockStream);
        Stream<Integer> flatStream = mergingStream.flatStream();
        // creating the stream should not call anything and be fully lazy
        verify(mockStream, times(1)).iterator();
        verify(mockIterator, times(0)).hasNext();
        verify(mockIterator, times(0)).next();
    }

    @Test
    public void innerStreamIsConsumedLazily() {
        Stream<Integer> mockFirstInnerStream = mock(Stream.class);
        Stream<Integer> mockSecondInnerStream = mock(Stream.class);

        Stream<Stream<Integer>> outerStream = Stream.of(mockFirstInnerStream, mockSecondInnerStream);

        Iterator<Integer> mockIntegerIterator = mock(Iterator.class);
        when(mockIntegerIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockIntegerIterator.next()).thenReturn(1).thenReturn(2);
        when(mockFirstInnerStream.iterator()).thenReturn(mockIntegerIterator);

        Iterator<Integer> mockSecondIntegerIterator = mock(Iterator.class);
        when(mockSecondIntegerIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockSecondIntegerIterator.next()).thenReturn(21).thenReturn(22).thenReturn(23);
        when(mockSecondInnerStream.iterator()).thenReturn(mockSecondIntegerIterator);

        LazyMergingStream<Integer> mergingStream = new LazyMergingStream<>(outerStream);
        Stream<Integer> flatStream = mergingStream.flatStream();

        List<Integer> numbers = flatStream.limit(3).collect(Collectors.toList());

        verify(mockIntegerIterator, times(2)).next();
        verify(mockIntegerIterator, times(3)).hasNext();

        verify(mockSecondIntegerIterator, times(1)).next();
        verify(mockSecondIntegerIterator, times(1)).hasNext();

        assertEquals(Arrays.asList(1, 2, 21), numbers);
    }
}
