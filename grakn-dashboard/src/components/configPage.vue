<!--
Grakn - A Distributed Semantic Database
Copyright (C) 2016  Grakn Labs Limited

Grakn is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Grakn is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
-->

<template>
<div class="container">
    <div class="inline-flex-1"></div>
    <div class="panel-body" v-if="response">
        <div class="table-responsive">
            <table class="table table-hover table-stripped">
                <thead>
                    <tr>
                        <th>Config Item</th>
                        <th>Value</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Hostname</td>
                        <td>{{response['server.host']}}</td>
                    </tr>
                    <tr>
                        <td>Server Port</td>
                        <td>{{response['server.port']}}</td>
                    </tr>
                    <tr>
                        <td>Threads</td>
                        <td>{{response['loader.threads']}}</td>
                    </tr>
                    <tr>
                        <td>Database config</td>
                        <td>{{response['graphdatabase.config']}}</td>
                    </tr>
                    <tr>
                        <td>Batch config</td>
                        <td>{{response['graphdatabase.batch-config']}}</td>
                    </tr>
                    <tr>
                        <td>Graph Computer config</td>
                        <td>{{response['graphdatabase.computer']}}</td>
                    </tr>
                    <tr>
                        <td>Engine assets directory</td>
                        <td>{{response['server.static-file-dir']}}</td>
                    </tr>
                    <tr>
                        <td>Log File</td>
                        <td>{{response['logging.file.main']}}</td>
                    </tr>
                    <tr>
                        <td>Logging Level</td>
                        <td>{{response['logging.level']}}</td>
                    </tr>
                    <tr>
                        <td>Background Tasks time lapse</td>
                        <td>{{response['backgroundTasks.time-lapse']}}</td>
                    </tr>
                    <tr>
                        <td>Background Tasks post processing delay</td>
                        <td>{{response['backgroundTasks.post-processing-delay']}}</td>
                    </tr>
                    <tr>
                        <td>Batch size</td>
                        <td>{{response['blockingLoader.batch-size']}}</td>
                    </tr>
                    <tr>
                        <td>Default Keyspace</td>
                        <td>{{response['graphdatabase.default-keyspace']}}</td>
                    </tr>
                    <tr>
                        <td>Repeat Commits</td>
                        <td>{{response['loader.repeat-commits']}}</td>
                    </tr>
                    <tr>
                        <td>HAL builder degree</td>
                        <td>{{response['halBuilder.degree']}}</td>
                    </tr>
                    <tr>
                        <td>Version</td>
                        <td>{{response['project.version']}}</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <div class="panel panel-filled panel-c-danger" v-else>
        <div class="panel-heading">
            Could not connect to Grakn
        </div>
        <div class="panel-body">
            Have you tried turning it off and on again?
            <pre class="error-pre" v-show="errorMessage">{{errorMessage}}</pre>
        </div>
        <div class="panel-footer">
            <button @click="retry" class="btn btn-default">Retry Connection<i class="pe-7s-refresh"></i></button>
        </div>
    </div>
    <div class="inline-flex-1"></div>
</div>
</template>

<style scoped>
table {
    border-collapse: separate;
    border-spacing: 15px;
}
th{
  font-weight: bold;
}
td{
  border-bottom: 1px solid #606060;
  padding: 5px;
}
.container {
    display: flex;
    flex-direction: row;
    justify-content: center;
    height: 100%;
    width: 100%;
    position: absolute;
}

.table-responsive{
  margin-top: 10px;
}

.inline-flex-1 {
    display: inline-flex;
    flex: 1;
}

.panel-body {
    display: inline-flex;
    justify-content: center;
    flex: 3;
    background-color: green;
    margin-top: 10px;
    margin-bottom: 25px;
    background-color: #0f0f0f;
    margin-left: 15px;
    margin-right: 15px;
    padding-top: 30px;
    overflow: scroll;
}
</style>

<script>
import EngineClient from '../js/EngineClient.js';

export default {
    name: "ConfigurationPage",
    data: () => {
        return {
            response: undefined,
            errorMessage: undefined,
        };
    },

    created() {},
    mounted() {
        this.$nextTick(function() {
            EngineClient.getConfig(this.engineStatus);
        });
    },

    methods: {
        showError(msg) {
            this.response = undefined;
            this.errorMessage = msg;
        },
        engineStatus(resp, err) {
            if (resp != null)
                this.response = resp
            else
                this.showError(err);
        },

        materialiseAll() {
            EngineClient.preMaterialiseAll();
        },

        retry() {
            EngineClient.getConfig(this.engineStatus);
        }
    }

}
</script>
