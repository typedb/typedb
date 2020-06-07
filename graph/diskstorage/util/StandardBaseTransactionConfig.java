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

package grakn.core.graph.diskstorage.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;

import java.time.Instant;

public class StandardBaseTransactionConfig implements BaseTransactionConfig {

    private volatile Instant commitTime;
    private final TimestampProvider times;
    private final Configuration customOptions;

    private StandardBaseTransactionConfig(TimestampProvider times, Instant commitTime, Configuration customOptions) {
        this.times = times;
        this.commitTime = commitTime;
        this.customOptions = customOptions;
    }

    @Override
    public synchronized Instant getCommitTime() {
        if (commitTime == null) {
            //set commit time to current time
            commitTime = times.getTime();
        }
        return commitTime;
    }

    @Override
    public synchronized void setCommitTime(Instant time) {
        Preconditions.checkArgument(commitTime == null, "A commit time has already been set");
        this.commitTime = time;
    }

    @Override
    public boolean hasCommitTime() {
        return commitTime != null;
    }

    @Override
    public TimestampProvider getTimestampProvider() {
        return times;
    }

    @Override
    public <V> V getCustomOption(ConfigOption<V> opt) {
        return customOptions.get(opt);
    }

    @Override
    public Configuration getCustomOptions() {
        return customOptions;
    }

    public static class Builder {

        private Instant commitTime = null;
        private TimestampProvider times;
        private Configuration customOptions = Configuration.EMPTY;

        public Builder() {
        }

        /**
         * Copies everything from {@code template} to this builder except for
         * the {@code commitTime}.
         *
         * @param template an existing transaction from which this builder will take
         *                 its values
         */
        public Builder(BaseTransactionConfig template) {
            customOptions(template.getCustomOptions());
            timestampProvider(template.getTimestampProvider());
        }

        public Builder commitTime(Instant commit) {
            commitTime = commit;
            return this;
        }

        public Builder timestampProvider(TimestampProvider times) {
            this.times = times;
            return this;
        }

        public Builder customOptions(Configuration c) {
            customOptions = c;
            Preconditions.checkNotNull(customOptions, "Null custom options disallowed; use an empty Configuration object instead");
            return this;
        }

        public StandardBaseTransactionConfig build() {
            return new StandardBaseTransactionConfig(times, commitTime, customOptions);
        }
    }

    public static StandardBaseTransactionConfig of(TimestampProvider times) {
        return new Builder().timestampProvider(times).build();
    }

    public static StandardBaseTransactionConfig of(TimestampProvider times, Configuration customOptions) {
        return new Builder().timestampProvider(times).customOptions(customOptions).build();
    }

}
