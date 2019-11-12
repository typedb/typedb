/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.hadoop.formats.cql;

import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class GraknCqlConfigHelper {

    private static final String INPUT_CQL_COLUMNS_CONFIG = "cassandra.input.columnfamily.columns";
    private static final String INPUT_CQL_WHERE_CLAUSE_CONFIG = "cassandra.input.where.clause";
    private static final String INPUT_CQL = "cassandra.input.cql";
    private static final String INPUT_NATIVE_AUTH_PROVIDER = "cassandra.input.native.auth.provider";
    private static final String USERNAME = "cassandra.username";
    private static final String PASSWORD = "cassandra.password";


    public static String getInputcolumns(Configuration conf) {
        return conf.get(INPUT_CQL_COLUMNS_CONFIG);
    }

    public static String getInputWhereClauses(Configuration conf) {
        return conf.get(INPUT_CQL_WHERE_CLAUSE_CONFIG);
    }

    public static String getInputCql(Configuration conf) {
        return conf.get(INPUT_CQL);
    }

    public static void setUserNameAndPassword(Configuration conf, String username, String password) {
        if (StringUtils.isNotBlank(username)) {
            conf.set(INPUT_NATIVE_AUTH_PROVIDER, PlainTextAuthProvider.class.getName());
            conf.set(USERNAME, username);
            conf.set(PASSWORD, password);
        }
    }
}
