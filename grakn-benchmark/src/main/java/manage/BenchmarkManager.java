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

package manage;

import executor.QueryExecutor;
import generator.DataGenerator;

public class BenchmarkManager {
    private DataGenerator dataGenerator;
    private QueryExecutor queryExecutor;
    private int numQueryRepetitions;

    public BenchmarkManager(DataGenerator dataGenerator, QueryExecutor queryExecutor, int numQueryRepetitions) {
        this.dataGenerator = dataGenerator;
        this.queryExecutor = queryExecutor;
        this.numQueryRepetitions = numQueryRepetitions;
    }

    public void run(int numConcepts) {
        this.dataGenerator.generate(numConcepts);
        this.queryExecutor.processStaticQueries(numQueryRepetitions, numConcepts);
    }


    public static void main(String[] args) {

        String uri = "localhost:48555";
        String keyspace = "societal_model";

        DataGenerator dataGenerator = new DataGenerator(keyspace, uri);
        QueryExecutor queryExecutor = new QueryExecutor(keyspace, uri);
        BenchmarkManager manager = new BenchmarkManager(dataGenerator, queryExecutor, 100);
        manager.run(100);
        manager.run(200);
        manager.run(300);
        manager.run(400);
//        manager.run(1000);
    }
}
