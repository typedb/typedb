<template>
    <div ref="VueTable"></div>
</template>

<style scoped>

</style>

<script>
  import { Table, Column, Cell } from '@blueprintjs/table';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueTable',
    props: ['numRows'],
    mounted() {
      this.$nextTick(() => {
        const cell = React.createElement(Cell, {
          className: 'vue-cell',
        }, 'hello');

        const col = React.createElement(Column, {
          className: 'vue-column',
          name: 'test',
          numRows: this.numRows,
          cellRenderer: () => cell,
        });


        ReactDom.render(React.createElement(Table, {
          className: 'vue-table',
          numRows: this.numRows,
          intent: 'primary',
        }, col), this.$refs.VueTable);
      });
    },
  };
</script>

