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

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.loader.BlockingLoader;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.RESTUtil;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

    //TODO: Use redis for caching LRU
    Map<String, String> entitiesMap;
    ArrayList<Var> relationshipsList;

    private BlockingLoader loader;


    public ImportController() {
        new ImportController(ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY));
    }

    public ImportController(String graphNameInit) {

        post(RESTUtil.WebPath.IMPORT_DATA_URI, (req, res) -> {
            try {
                JSONObject bodyObject = new JSONObject(req.body());
                importDataFromFile(bodyObject.get(RESTUtil.Request.PATH_FIELD).toString());
            } catch (JSONException j) {
                res.status(400);
                return j.getMessage();
            } catch (Exception e) {
                res.status(500);
                return e.getMessage();
            }
            return "";
        });

        post(RESTUtil.WebPath.IMPORT_ONTOLOGY_URI, (req, res) -> {
            try {
                JSONObject bodyObject = new JSONObject(req.body());
                importOntologyFromFile(bodyObject.get(RESTUtil.Request.PATH_FIELD).toString());
            } catch (JSONException j) {
                res.status(400);
                return j.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                res.status(500);
                return e.getMessage();
            }
            return "";
        });

        entitiesMap = new ConcurrentHashMap<>();
        relationshipsList = new ArrayList<>();
        batchSize = ConfigProperties.getInstance().getPropertyAsInt(ConfigProperties.BATCH_SIZE_PROPERTY);
        graphName = graphNameInit;
        loader = new BlockingLoader(graphName);

    }

    public void importDataFromFile(String dataFile) throws IOException {
        scanFile(this::parseEntity, dataFile);
        loader.waitToFinish();
        scanFile(this::parseRelation, dataFile);
        loader.waitToFinish();
    }


    private void scanFile(BiPredicate<String, List<Var>> parser, String dataFile) throws IOException, IllegalArgumentException {

        int i = 0;
        int latestBatchNumber = 0;
        String line;
        List<Var> currentVarsBatch = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));

        // TODO: as soon as it is available use Graql method that given an input stream provides .nextPattern.

        while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("insert")) line = line.substring(6);

            //Skip empty lines && comments
            if (line.length() > 0 && !line.startsWith("#") && parser.test(line, currentVarsBatch))
                i++;

            if (i % batchSize == 0 && latestBatchNumber != i) {
                latestBatchNumber = i;
                loader.addToQueue(currentVarsBatch);
                LOG.info("New batch:  " + i);
                currentVarsBatch = new ArrayList<>();
            }
        }

        //Digest the remaining Vars in the batch.

        if (currentVarsBatch.size() > 0) {
            loader.addToQueue(currentVarsBatch);
            LOG.info("New batch:  " + i);
        }

        bufferedReader.close();
    }

    //TODO: refactor the two following methods

    private boolean parseEntity(String line, List<Var> currentVarsBatch) throws IllegalArgumentException {
        Var var;
        try {
            var = (Var) QueryParser.create().parseInsertQuery("insert " + line).admin().getVars().toArray()[0];
        } catch (IllegalArgumentException e) {
            LOG.error(ErrorMessage.PARSING_EXCEPTION.getMessage(line));
            throw e;
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

    private boolean parseRelation(String line, List<Var> currentVarsBatch) throws IllegalArgumentException {
        // if both role players have id in the cache then substitute the var to var().id(map.get(variable))
        Var var;
        try {
            var = (Var) QueryParser.create().parseInsertQuery("insert " + line).admin().getVars().toArray()[0];
        } catch (IllegalArgumentException e) {
            LOG.error(ErrorMessage.PARSING_EXCEPTION.getMessage(line));
            throw e;
        }

        boolean ready = false;
        if (var.admin().isRelation()) {
            ready = true;

            for (Var.Casting x : var.admin().getCastings()) {
                if (x.getRolePlayer().admin().isUserDefinedName()) {
                    if (entitiesMap.containsKey(x.getRolePlayer().getName()))
                        x.getRolePlayer().id(entitiesMap.get(x.getRolePlayer().getName()));
                    else
                        return false;
                }
            }
            currentVarsBatch.add(var);
        }
        return ready;

    }

    public void importOntologyFromFile(String ontologyFile) throws IOException, MindmapsValidationException{

        MindmapsTransaction transaction = GraphFactory.getInstance().getGraph(graphName).newTransaction();

        LOG.info("Loading new ontology .. ");

        List<String> lines = Files.readAllLines(Paths.get(ontologyFile), StandardCharsets.UTF_8);
        String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
        QueryParser.create(transaction).parseInsertQuery(query).execute();
        transaction.commit();

        LOG.info("Ontology loaded. ");

    }
}
