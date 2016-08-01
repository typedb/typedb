/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.api;

import io.mindmaps.util.ConfigProperties;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.loader.BlockingLoader;
import io.mindmaps.util.RESTUtil;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;

import static spark.Spark.post;

/**
 * Class that provides methods to import ontologies and data from a Graql file to a graph.
 */

public class ImportController {

    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(ImportController.class);


    private int batchSize;
    private String graphName;

    //Use redis for caching LRU
    Map<String, String> entitiesMap;
    ArrayList<Var> relationshipsList;

    private BlockingLoader loader;


    public ImportController() {
        new ImportController(ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY));
    }

    public ImportController(String graphNameInit) {

        // These two functions still block. Create new thread

        post(RESTUtil.WebPath.IMPORT_DATA_URI, (req, res) -> {
            JSONObject bodyObject = new JSONObject(req.body());
            importDataFromFile(bodyObject.get(RESTUtil.Request.PATH_FIELD).toString());
            return null;
        });

        post(RESTUtil.WebPath.IMPORT_ONTOLOGY_URI, (req, res) -> {
            JSONObject bodyObject = new JSONObject(req.body());
            loadOntologyFromFile(bodyObject.get(RESTUtil.Request.PATH_FIELD).toString());
            return null;
        });

        entitiesMap = new ConcurrentHashMap<>();
        relationshipsList = new ArrayList<>();
        batchSize = ConfigProperties.getInstance().getPropertyAsInt(ConfigProperties.BATCH_SIZE_PROPERTY);
        graphName = graphNameInit;
        loader = new BlockingLoader(graphName);

    }

    public void importDataFromFile(String dataFile) {
        try {
            scanFile(this::parseEntity, dataFile);
            loader.waitToFinish();
            scanFile(this::parseRelation, dataFile);
            loader.waitToFinish();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void scanFile(BiPredicate<String, List<Var>> parser, String dataFile) throws IOException {

        int i = 0;
        int latestBatchNumber = 0;
        String line;
        List<Var> currentVarsBatch = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));

        // Instead of reading one line at the time, in the future we will have a
        // Graql method that given an input stream provides .nextPattern.

        while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("insert")) line = line.substring(6);

            //Skip empty lines && comments
            if (line.length() > 0 && !line.startsWith("#") && parser.test(line, currentVarsBatch))
                i++;

            if (i % batchSize == 0 && latestBatchNumber != i) {
                latestBatchNumber = i;
                loader.addToQueue(currentVarsBatch);
                LOG.info("[ New batch:  " + i + " ]");
                currentVarsBatch = new ArrayList<>();
            }
        }

        //Digest the remaining Vars in the batch.

        if (currentVarsBatch.size() > 0) {
            loader.addToQueue(currentVarsBatch);
            LOG.info("[ New batch:  " + i + " ]");
        }

        bufferedReader.close();
    }

    private boolean parseEntity(String command, List<Var> currentVarsBatch) {
        Var var = null;
        try {
            var = (Var) QueryParser.create().parseInsertQuery("insert " + command).admin().getVars().toArray()[0];
            //catch more precise exception
        } catch (IllegalArgumentException e) {
            LOG.error("Exception caused by " + command);
            e.printStackTrace();
        }

        if (!entitiesMap.containsKey(var.admin().getName()) && !var.admin().isRelation() && var.admin().getType().isPresent()) {

                if (var.admin().isUserDefinedName()) {
                    String varId = (var.admin().getId().isPresent()) ? var.admin().getId().get() : UUID.randomUUID().toString();
                    entitiesMap.put(var.admin().getName(), varId); // add check for var name.
                    currentVarsBatch.add(var.admin().id(varId));
                } else {
                    currentVarsBatch.add(var);
                }

                return true;
            }
        return false;
    }

    private boolean parseRelation(String command, List<Var> currentVarsBatch) {
        // if both role players have id in the cache then substitute the var to var().id(map.get(variable))
        try {
            Var var = (Var) QueryParser.create().parseInsertQuery("insert " + command).admin().getVars().toArray()[0];
            boolean ready = false;
            if (var.admin().isRelation()) {
                ready = true;

                for (Var.Casting x : var.admin().getCastings()) {
                    if (!x.getRolePlayer().admin().isUserDefinedName())
                        continue; ///aaahhh very ugly
                    if (entitiesMap.containsKey(x.getRolePlayer().getName()))
                        x.getRolePlayer().id(entitiesMap.get(x.getRolePlayer().getName()));
                    else
                        return false; /// aaahhhhhh
                }
                currentVarsBatch.add(var);
            }
            return ready;
            //fix execption
        } catch (Exception e) {
            LOG.error("Exception caused by " + command);
            e.printStackTrace();
            return false;
        }

    }

    public void loadOntologyFromFile(String ontologyFile) {

        MindmapsTransaction transaction = GraphFactory.getInstance().getGraph(graphName).newTransaction();

        try {
            LOG.info("[ Loading new ontology .. ]");

            List<String> lines = Files.readAllLines(Paths.get(ontologyFile), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            QueryParser.create(transaction).parseInsertQuery(query).execute();
            transaction.commit();

            LOG.info("[ Ontology loaded. ]");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
