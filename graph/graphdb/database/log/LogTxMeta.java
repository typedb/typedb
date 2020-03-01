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

package grakn.core.graph.graphdb.database.log;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.TransactionBuilder;
import grakn.core.graph.graphdb.transaction.TransactionConfiguration;


public enum LogTxMeta {

    LOG_ID {
        @Override
        public Object getValue(TransactionConfiguration txConfig) {
            return txConfig.getLogIdentifier();
        }

        @Override
        public void setValue(TransactionBuilder builder, Object value) {
            Preconditions.checkArgument(value != null && (value instanceof String));
            builder.logIdentifier((String) value);
        }

        @Override
        public Class dataType() {
            return String.class;
        }
    };

    public abstract Object getValue(TransactionConfiguration txConfig);

    public abstract void setValue(TransactionBuilder builder, Object value);

    public abstract Class dataType();

}
