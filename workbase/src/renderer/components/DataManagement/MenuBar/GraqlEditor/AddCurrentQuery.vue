<template>
  <div v-if="currentQuery" id="addCurrentQuery">
    <img @click="toggleToolTip" src="static/img/icons/icon_add_white.svg" class="btn add-query-btn" id="add-query-btn" :class="{'btn-selected':toolTipShown === 'addCurrentQuery'}">
    <transition name="slide-fade">
      <div v-if="toolTipShown === 'addCurrentQuery'" class="tooltip-arrow-box z-depth-3">
        <div>
          <p>Save current query</p>
        </div>
        <div class="tooltip-body">
          <div><input type="text" id="query-name" class="grakn-input" v-model="currentQueryName" placeholder="query name" v-focus></div>
          <div><button id="save-fav-query" @click="addFavQuery" class="btn btn-default" :disabled="currentQueryName.length==0">Save</button></div>
        </div>
      </div>
    </transition>
  </div>
</template>

<style scoped>
.tooltip-body{
  display: flex;
  flex-direction: row;
  align-items: center;
}

/*Transition for the arrow box*/

.slide-fade-enter-active {
    transition: all .6s ease;
}
.slide-fade-leave-active {
    transition: all .3s cubic-bezier(1.0, 0.5, 0.8, 1.0);
}
.slide-fade-enter,
.slide-fade-leave-active {
    transform: translateY(-10px);
    opacity: 0;
}

/*Icon with plus sign*/
div>span {
    font-size: 25px;
    cursor: pointer;
}
/*Tooltip positioning*/

#addCurrentQuery {
  display: inline-flex;
  margin: auto;
}
.tooltip-arrow-box:after,
.tooltip-arrow-box:before {
    bottom: 100%;
    left: 90%;
    border: solid transparent;
    content: " ";
    height: 0;
    width: 0;
    position: absolute;
    pointer-events: none;
}

.tooltip-arrow-box:after {
    position: absolute;
    border-color: rgba(136, 183, 213, 0);
    border-bottom-color: #282828;
    border-width: 8px;
}


.tooltip-arrow-box {
    padding: 5px 10px;
    background-color:#282828;
    text-align: center;
    border-radius: 2px;
    position: absolute;
    z-index: 1;
    top: 120%;
    right: 21%;
    display: flex;
    flex-direction: column;
}

.add-query-btn {
  padding: 0px;
  width: 30px;
  height: 30px;
  border-radius: 50%;
}
</style>

<script>

export default {
  name: 'addQueryButton',
  props: ['currentQuery', 'toolTipShown'],
  data() {
    return {
      currentQueryName: '',
    };
  },
  directives: {
    // Registering local directive to always force focus on query-name input
    focus: {
      inserted(el) {
        el.focus();
      },
    },
  },
  created() {
    // Global key bindings
    window.addEventListener('keyup', (e) => {
      if (e.keyCode === 13 && !e.shiftKey && (this.toolTipShown === 'addCurrentQuery')) this.addFavQuery();
    });
  },
  watch: {
    currentQuery(val) {
      if (!val.length) this.$emit('toggle-tool-tip');
    },
  },
  methods: {
    toggleToolTip() {
      if (!(this.toolTipShown === 'addCurrentQuery')) {
        this.$emit('toggle-tool-tip', 'addCurrentQuery');
      } else {
        this.$emit('toggle-tool-tip');
      }
    },
    addFavQuery() {
      this.$emit('new-fav-query', this.currentQueryName);
      this.$notifySuccess('New query saved!');
      this.$emit('toggle-tool-tip');
      this.currentQueryName = '';
    },
  },
};
</script>
