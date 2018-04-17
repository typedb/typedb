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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.kb.log.CommitLog;
import com.google.auto.value.AutoValue;

/**
 * <p>
 *     Simple helper class which contains {@link IndexPostProcessor} and {@link CountPostProcessor}.
 *     This is so we can hold all the logic for post processing in one place without encapsulating too much
 *     diverging logic in one class
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class PostProcessor {
    public abstract IndexPostProcessor index();
    public abstract CountPostProcessor count();

    public static PostProcessor create(IndexPostProcessor indexPostProcessor, CountPostProcessor countPostProcessor) {
        return new AutoValue_PostProcessor(indexPostProcessor, countPostProcessor);
    }

    /**
     * Submits a {@link CommitLog} to be stored and post processed later
     *
     * @param commitLog The {@link CommitLog} to store for usage later
     */
    public void submit(CommitLog commitLog){
        index().updateIndices(commitLog);
        count().updateCounts(commitLog);
    }
}
