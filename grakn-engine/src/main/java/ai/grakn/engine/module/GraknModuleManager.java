/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.engine.module;

import ai.grakn.graknmodule.GraknModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.JarPluginManager;
import ro.fortsoft.pf4j.PluginManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static ai.grakn.engine.module.GraknModuleHelper.listFolders;
import static ai.grakn.engine.module.GraknModuleHelper.listJarFiles;
/**
 * Grakn Module Manager class.
 * Responsible for module initialization and provides access to available modules
 *
 * @author Ganeshwara Herawan Hananda
 */
public class GraknModuleManager {
    private static final String MODULE_DIR = "./modules";

    private static final Logger LOG = LoggerFactory.getLogger(GraknModuleManager.class);
    private static PluginManager pf4jPluginManager = new JarPluginManager();

    public static void initialise() {
        String moduleDirAbsolute = Paths.get(MODULE_DIR).toAbsolutePath().toString();
        LOG.info("Scanning Grakn modules directory at '{}'", moduleDirAbsolute);
        try {
            Stream<Path> folders = listFolders(Paths.get(MODULE_DIR));
            Stream<String> pf4jPluginIds = folders.flatMap(GraknModuleManager::scanModuleDir);
            pf4jPluginIds.forEach(GraknModuleManager::startPf4JPlugin);
        } catch (RuntimeException e) {
            throw GraknModuleException.exception("Unable to scan module directory at '" + moduleDirAbsolute + "'", e);
        }
    }

    public static List<GraknModule> getGraknModules() {
        try {
            List<GraknModule> modules = pf4jPluginManager.getExtensions(GraknModule.class);
            LOG.info("Number of Grakn module(s) found: {}", modules.size());
            for (GraknModule module : modules) {
                LOG.info("Module '{}' loaded", module.getGraknModuleName());
            }

            return modules;
        } catch (RuntimeException e) {
            throw GraknModuleException.exception("Unable to scan module directory", e);
        }
    }

    private static Stream<String> scanModuleDir(Path moduleDir) {
        LOG.info("Scanning '{}' directory for a module", moduleDir.getFileName());

        Stream<String> pluginIds = listJarFiles(moduleDir).map(moduleJar -> {
            LOG.info("Found '{}'", moduleJar.getFileName());
            try {
               return pf4jPluginManager.loadPlugin(moduleJar);
            } catch (RuntimeException e) {
                throw GraknModuleException.exception("Unable to load module '" + moduleJar + "'", e);
            }
        });

        return pluginIds;
    }

    private static void startPf4JPlugin(String pf4jPluginId) {
        LOG.info("Starting module '{}'", pf4jPluginId);
        try {
            pf4jPluginManager.startPlugin(pf4jPluginId);
        } catch (RuntimeException e) {
            throw GraknModuleException.exception("Unable to start module '" + pf4jPluginId + "'", e);
        }
    }
}

