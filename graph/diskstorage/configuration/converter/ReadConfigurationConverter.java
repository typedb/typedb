// Copyright 2018 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.diskstorage.configuration.converter;

import org.apache.commons.configuration.BaseConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;

/**
 * Converter from {@link ReadConfiguration} into {@link BaseConfiguration}
 */
public class ReadConfigurationConverter {

    private static org.janusgraph.diskstorage.configuration.converter.ReadConfigurationConverter configurationConverter;

    private ReadConfigurationConverter(){}

    public static org.janusgraph.diskstorage.configuration.converter.ReadConfigurationConverter getInstance(){
        if(configurationConverter == null){
            configurationConverter = new org.janusgraph.diskstorage.configuration.converter.ReadConfigurationConverter();
        }
        return configurationConverter;
    }

    public BaseConfiguration convert(ReadConfiguration readConfiguration) {
        BaseConfiguration result = new BaseConfiguration();
        for (String k : readConfiguration.getKeys("")) {
            result.setProperty(k, readConfiguration.get(k, Object.class));
        }
        return result;
    }

}
