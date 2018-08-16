/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package manage;

import executor.QueryExecutor;
import generator.DataGenerator;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 *
 */
public class BenchmarkManager {
    private DataGenerator dataGenerator;
    private QueryExecutor queryExecutor;
    private int numQueryRepetitions;

    public BenchmarkManager(DataGenerator dataGenerator, QueryExecutor queryExecutor, int numQueryRepetitions) {
        this.dataGenerator = dataGenerator;
        this.queryExecutor = queryExecutor;
        this.numQueryRepetitions = numQueryRepetitions;
    }

    public void run(List<Integer> numConceptsInRun) {
        Number timestamp = this.runStartDateTime();
        for (int numConcepts : numConceptsInRun) {
            this.dataGenerator.generate(numConcepts);
            this.queryExecutor.processStaticQueries(numQueryRepetitions, numConcepts, timestamp);
        }
    }

    /**
     * Used to generate a timestamp for each benchmarking run
     * @return
     */
    private Number runStartDateTime(){
        return new Date().getTime();
    }

    public static void main(String[] args) {

        String uri = "localhost:48555";
        String keyspace = "societal_model";
        String dataSetName = "generated_societal_model";

        DataGenerator dataGenerator = new DataGenerator(keyspace, uri);
        QueryExecutor queryExecutor = new QueryExecutor(keyspace, uri, dataSetName);
        BenchmarkManager manager = new BenchmarkManager(dataGenerator, queryExecutor, 100);

        List<Integer> numConceptsInRun = Arrays.asList(100, 250);
        manager.run(numConceptsInRun);
    }
}
