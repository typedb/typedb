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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.implementation.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Var;
import io.mindmaps.loader.DistributedLoader;
import io.mindmaps.util.ConfigProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DistributedLoaderTest {

    Properties prop = new Properties();
    String graphName;
    DistributedLoader loader;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(DistributedLoaderTest.class);


    @Before
    public void setUp() throws Exception {
        new TransactionController();
        try {
            prop.load(DistributedLoaderTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        graphName = prop.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        loader= new DistributedLoader(graphName, Collections.singletonList("localhost"));
        new CommitLogController();
    }

    @Test
    public void testLoadOntologyAndData() {
        Logger logger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);

        loadOntology();

        ClassLoader classLoader = getClass().getClassLoader();
        File fileData= new File(classLoader.getResource("small_nametags.gql").getFile());
        loader.setBatchSize(10);
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(fileData)).forEach(pattern -> loader.addToQueue(pattern.admin().asVar()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        loader.waitToFinish();
        Assert.assertNotNull(GraphFactory.getInstance().getGraph(graphName).newTransaction().getConcept("X546f736869616b69204b61776173616b69").getId());
    }

    private void loadOntology(){
        MindmapsTransaction transaction = GraphFactory.getInstance().getGraph(graphName).newTransaction();
        List<Var> ontologyBatch = new ArrayList<>();
        ClassLoader classLoader = getClass().getClassLoader();

        LOG.info("Loading new ontology .. ");
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(classLoader.getResource("dblp-ontology.gql").getFile())).map(x->x.admin().asVar()).forEach(ontologyBatch::add);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        QueryBuilder.build(transaction).insert(ontologyBatch).execute();
        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }

        LOG.info("Ontology loaded. ");
    }

    @After
    public void cleanGraph(){
        GraphFactory.getInstance().getGraph(graphName).clear();
    }

}