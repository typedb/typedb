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
<section class="wrapper">
    <!-- Main content-->
    <section class="content">
        <router-view></router-view>
    </section>

    <!-- End main content-->
</section>
</template>

<style>

</style>

<script>
import EngineClient from '../js/EngineClient.js';

export default {
    data: function() {
        return {
            version: undefined,
            engineClient: {}
        }
    },
    created: function() {
        this.engineClient = new EngineClient();
        window.useReasoner = false;
    },
    mounted: function() {
        this.$nextTick(function() {
            this.engineClient.getConfig((r, e) => {
                this.version = (r == null ? 'error' : r['project.version'])
            });
        })
    }
}
</script>
