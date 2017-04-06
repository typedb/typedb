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
<div id="tool-tip" class="node-panel z-depth-1-half noselect" v-show="showToolTip">
        <div class="dd-item"><span class="list-key"><font>{{nodeType}}</font></span></div>
</div>
</template>

<style scoped>
.node-panel {
    z-index: 1;
    position: absolute;
    background-color: #0f0f0f;
    padding: 4px;
    border-radius: 3px;
}

font {
    font-weight: bold;
}

.dd-item {
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 98%;
    margin: 5px;
}
</style>

<script>
export default {
  name: 'NodeToolTip',
  props: ['showToolTip', 'mouseEvent', 'graphOffsetTop'],
  data () {
      return {
          nodeType: '',
          toolTipElement: undefined,
        };
    },
  watch: {
      showToolTip(newValue) {
          if (newValue) {
              const nodeId = this.mouseEvent.node;
              const nodeDOMCoordinates = visualiser.network.canvasToDOM(visualiser.network.getPositions(nodeId)[nodeId]);
              const nodeBoundingBox = visualiser.network.canvasToDOM({ x: 0, y: visualiser.network.getBoundingBox(nodeId).top });

              const offsetX = $('#tool-tip').width() / 2;
              const offsetY = $('#tool-tip').height() + 10;

              this.toolTipElement.style.left = `${nodeDOMCoordinates.x - offsetX  }px`;
              this.toolTipElement.style.top = `${nodeBoundingBox.y - offsetY }px`;
              const nodeObj = visualiser.getNode(nodeId);
              this.nodeType = (nodeObj.type !== '') ? nodeObj.type : nodeObj.baseType;
            }
        },
    },
  mounted () {
      this.$nextTick(function nextTickVisualiser() {
        this.toolTipElement = document.getElementById('tool-tip');
      });
    },
  methods: {},
};
</script>
