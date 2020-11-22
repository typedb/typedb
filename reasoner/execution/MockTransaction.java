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

package grakn.core.reasoner.execution;

import java.util.Iterator;
import java.util.List;

import static grakn.common.collection.Collections.list;

/**
 * Pretend to compute an iterator of longs, which just adds up numbers and returns
 * them in intervals.
 *
 * Repeatedly calling query() will repeat the same calculation loop in a new iterator
 *
 *
 * TODO this will get removed as we integrate the execution model into the main codebase
 */
public class MockTransaction {
    private final long computeLength;
    private final Long traversalPattern;
    private final int conceptMapInterval;

    public MockTransaction(long computeLength, Long traversalPattern, int conceptMapInterval) {
        this.computeLength = computeLength + traversalPattern;
        this.traversalPattern = traversalPattern;
        this.conceptMapInterval = conceptMapInterval;
    }

    public Iterator<List<Long>> query(List<Long> partialConceptMap) {
        return new Iterator<List<Long>>() {
            long count = traversalPattern;

            @Override
            public boolean hasNext() {
                return count < computeLength;
            }

            @Override
            public List<Long> next() {
                while (count < computeLength) {
                    if (count % conceptMapInterval == 0) {
                        List<Long> conceptMap = list(partialConceptMap, count);
                        count++;
                        return conceptMap;
                    } else {
                        count++;
                    }
                }
                return null;
            }
        };
    }
}
