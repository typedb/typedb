<!--
GRAKN.AI - THE KNOWLEDGE GRAPH
Copyright (C) 2018 Grakn Labs Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
-->

<template>
<transition name="slideInDown" appear>
    <div class="z-depth-1 message-panel" v-if="showMessagePanel">
        <div style="word-break: break-all">{{message}}</div><button @click="$emit('close-message')" class="btn">X</button>
    </div>
</transition>
</template>

<style scoped>
.message-panel {
    position: absolute;
    top: 100%;
    padding: 5px 15px;
    display: flex;
    width: 100%;
    flex-direction: row;
    z-index: 0;
    color: white;
    background-color: rgba(214, 4, 56, 1);
    margin-top: 10px;
    white-space: pre;
    font-family: Consolas, Monaco, 'Andale Mono', 'Ubuntu Mono', monospace;
    font-size: 90%;
}

button{
  margin-left: auto;
  margin-right: 0px;
  padding: 1px 3px;
  background-color: #cb0032;
}

i
{
  font-size: 100%;
}


</style>
<script>
export default {
  name: 'MessagePanel',
  props: ['message', 'showMessagePanel'],
  mounted() {
    this.$nextTick(() => {});
  },
  methods: {
  },
};
</script>
