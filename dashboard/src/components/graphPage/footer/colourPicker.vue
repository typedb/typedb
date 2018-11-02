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
    <div class="picker-column">
        <div class="selector-square red" @click="setColourOnType('#C84343')"></div>
        <div class="selector-square blue" @click="setColourOnType('#0CA1CF')"></div>
        <div class="selector-square green" @click="setColourOnType('#279d5d')"></div>
        <div class="selector-square yellow" @click="setColourOnType('#b3a41e')"></div>
    </div>
</template>

<style scoped>
.selector-square{
  height: 30px;
  width: 30px;
  cursor: pointer;
  margin-bottom: 5px;
}

.default{
  border: 1px solid white;
  color: white;
}

.red{
  background-color: #ff7878;
}

.blue{
  background-color:  #0CA1CF;
}
.green{
  background-color: #279d5d;
}

.yellow{
  background-color: #b3a41e;
}

.picker-column{
  display: flex;
  flex-direction: column;
  margin-left: 15px;
  margin-top: 3px;
}

</style>

<script>

export default {
  name: 'ColourPicker',
  props: ['node'],
  data() {
    return {

    };
  },
  created() {
  },
  mounted() {
    this.$nextTick(() => {

    });
  },
  methods: {
    setColourOnType(colourString) {
      visualiser.setColourOnNodeType(this.node.baseType, this.node.type, colourString);
    },
  },
};
</script>
