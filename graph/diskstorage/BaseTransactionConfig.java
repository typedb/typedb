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

package grakn.core.graph.diskstorage;

import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;

import java.time.Instant;

public interface BaseTransactionConfig {

    /**
     * Returns the commit time of this transaction which is either a custom timestamp provided
     * by the user, the commit time as set by the enclosing operation, or the first time this method is called.
     *
     * @return commit timestamp for this transaction
     */
    Instant getCommitTime();

    /**
     * Sets the commit time of this transaction. If a commit time has already been set, this method throws
     * an exception. Use #hasCommitTime() to check prior to setting.
     */
    void setCommitTime(Instant time);

    /**
     * Returns true if a commit time has been set on this transaction.
     */
    boolean hasCommitTime();

    /**
     * Returns the timestamp provider of this transaction.
     */
    TimestampProvider getTimestampProvider();

    /**
     * Get an arbitrary transaction-specific option.
     *
     * @param opt option for which to return a value
     * @return value of the option
     */
    <V> V getCustomOption(ConfigOption<V> opt);

    /**
     * Return any transaction-specific options.
     *
     * @return options for this tx
     * see #getCustomOption(ConfigOption)
     */
    Configuration getCustomOptions();
}
