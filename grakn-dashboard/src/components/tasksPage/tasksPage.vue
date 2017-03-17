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
    <div class="panel-body">
        <div class="table-div">
            <table cellspacing="0">
                <thead>
                    <tr class="sticky-top">
                        <th class="noselect" :class="{ active: sortKey == key.value }" v-for="key in columns">
                            <div @click="sortBy(key.value)">{{ key.label }}
                                <span class="arrow" :class="sortOrders[key.value] > 0 ? 'asc' : 'dsc'"></span></div><br>
                            <div class="filter-row"><input v-model="filtersMap[key.value]" :placeholder="key.label"></div>
                        </th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="(task,index) in filteredData">
                        <td v-for="field in columns">{{task[field.value]}}</td>
                        <td class="tabel-td"><button v-if="task.stoppable" @click="stopTask(task.id)" class='btn'>STOP</button></td>
                    </tr>

                </tbody>
            </table>
        </div>
        <div>
        </div>
    </div>
    <div class="inline-flex-1"></div>
</div>
</template>

<style scoped>
.sticky-top {
    position: -webkit-sticky;
    position: -moz-sticky;
    position: -ms-sticky;
    position: -o-sticky;
    position: sticky;
    position: sticky;
    top: 0px;
    background-color: #0f0f0f;
}

table {
    display: table;
}

.filter-row {
    display: inline-flex;
    justify-content: flex-end;
}

th {
    font-weight: bold;
    cursor: pointer;
    padding-bottom: 20px;
}

td {
    height: 40px;
    padding: 3px;
    text-align: center;
    vertical-align: middle;
    border-bottom: 1px solid #606060;
}


.table-div {
    display: flex;
    margin: 15px auto;
    overflow: scroll;
}

.arrow {
    display: inline-block;
    vertical-align: middle;
    width: 0;
    height: 0;
    margin-left: 5px;
    opacity: 0.66;
}

.arrow.asc {
    border-left: 4px solid transparent;
    border-right: 4px solid transparent;
    border-bottom: 4px solid #fff;
}

.arrow.dsc {
    border-left: 4px solid transparent;
    border-right: 4px solid transparent;
    border-top: 4px solid #fff;
}

.container {
    display: flex;
    flex-direction: row;
    justify-content: center;
    height: 100%;
    width: 100%;
    position: absolute;
}

.table-body {
    display: flex;
    flex-direction: column;
}

.inline-flex-1 {
    display: inline-flex;
    flex: 1;
}

.panel-body {
    display: flex;
    flex-flow: column;
    align-items: center;
    flex: 5;
    background-color: green;
    margin-top: 10px;
    margin-bottom: 15px;
    background-color: #0f0f0f;
    margin-left: 15px;
    margin-right: 15px;
    padding: 10px 15px;
}
</style>

<script>
import EngineClient from '../../js/EngineClient.js';

export default {
    name: "TasksPage",
    data: () => {
        let sortOrders = {};
        let columns = [{
            label: "Creator",
            value: "creator"
        }, {
            label: "Run at",
            value: 'runAt'
        }, {
            label: "Recurring",
            value: 'recurring'
        }, {
            label: "Class name",
            value: 'className'
        }, {
            label: "Status",
            value: 'status'
        }];
        let stoppableStatus = {
            CREATED: true,
            RUNNING: true,
        }
        let filtersMap = {
            creator: "",
            runAt: "",
            recurring: "",
            className: "",
            status: "",
        }
        columns.forEach(function(key) {
            sortOrders[key.value] = 1;
        })

        return {
            tasksArray: [],
            columns,
            sortKey: '',
            sortOrders: sortOrders,
            filtersMap,
            stoppableStatus,
        };
    },
    computed: {
        filteredData: function() {
            const sortKey = this.sortKey;
            const order = this.sortOrders[sortKey] || 1;
            let data = this.tasksArray;
            data = data.filter((row) => {
                return Object.keys(row).every((key) => {
                    if (key in this.filtersMap && this.filtersMap[key].length) {
                        return String(row[key]).toLowerCase().indexOf(this.filtersMap[key]) > -1;
                    } else {
                        return true;
                    }
                });
            })

            if (sortKey) {
                data = data.slice().sort(function compare(a, b) {
                    a = a[sortKey];
                    b = b[sortKey];
                    return (a === b ? 0 : a > b ? 1 : -1) * order;
                })
            }
            return data;
        }
    },
    created() {},
    mounted() {
        this.$nextTick(function() {
            this.requestAllTasks();
        });
    },

    methods: {
        sortBy(key) {
            this.sortKey = key;
            this.sortOrders[key] = this.sortOrders[key] * -1;
        },
        requestAllTasks() {
            EngineClient.getAllTasks().then(this.populateTasksTable, (error) => {
                console.log(error);
            });
        },
        populateTasksTable(response) {
            this.tasksArray = [];
            JSON.parse(response).forEach((task) => {
                this.tasksArray.push(Object.assign(task, this.createActionButtons(task.status)));
            });
        },
        createActionButtons(status) {
            return {
                stoppable: (status in this.stoppableStatus)
            };
        },
        stopTask(id) {
            EngineClient.stopTask(id).then(this.requestAllTasks);
        }
    }

}
</script>
