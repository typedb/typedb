<template>
<div>
    <button @click="loadFavQueries" class="btn top-bar-btn" :class="{'disabled':currentKeyspace, 'btn-selected':toolTipShown === 'favourites'}" id="fav-queries-btn">
        <img src="static/img/icons/icon_star.svg">
    </button>
    <transition name="slide-fade">
        <div v-if="toolTipShown === 'favourites'" class="dropdown-content" id="fav-queries-list">
            <i @click="closeFavQueriesList" :style="'font-size:13px;'" class="fas fa-times cross"></i>
            <div class="panel-heading">
                <h4 :style="'padding-right:10px;'">saved queries</h4>
            </div>
            <div class="divide"></div>
            <div class="panel-body" v-if="favQueries.length">
                <div class="dd-item" v-for="(query,index) in favQueries" :key="index">
                    <div class="full-query">
                        <span class="list-key" id="list-key"> {{query.name}}</span>
                    </div>
                    <div class="line-buttons">
                        <button id='use-btn' class="btn use-btn" @click="typeFavQuery(query.value)">USE</button>
                        <button id='delete-btn' class="btn delete-btn" @click="removeFavQuery(index, query.name)"><i class="fas fa-trash-alt"></i></button>
                    </div>
                </div>
            </div>
            <div class="panel-body" v-else>
                <div class="dd-item">
                    <div class="no-saved" id="no-saved">
                        no saved queries
                    </div>
                </div>
            </div>
        </div>
    </transition>
</div>
</template>

<style scoped>
.disabled{
    opacity:0.5;
    cursor: default;
}

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

.panel-filled {
    background-color: rgba(68, 70, 79, 1);
}

.bold {
    font-weight: bold;
}

.no-saved {
    margin-bottom: 15px;
}

.divide {
  border-bottom: 1px solid #606060;
  margin-top: 5px;
  margin-bottom: 5px;
}

.use-btn {
    padding: 7px;
    height: 30px;
    line-height: 1em;
}
.delete-btn {
    padding: 7px;
    height: 30px;
    line-height: 1em;
}

a {
    margin-left: auto;
}

.fa-times{
    cursor: pointer;
    position: absolute;
    right: 1px;
    top: 1px;
    padding: 2px;
    height: 14px;
    line-height: 1em;
}

.fa-times:hover{
    color: #06b17b;
}

.panel-heading {
    margin-bottom: 10px;
    font-size: 18px;
    text-align: center;
}

.page-header-icon {
    font-size: 20px;
    float: left;
    margin-right: 10px;
}

.panel-body {
    display: flex;
    flex-direction: column;
}

.fa-trash-alt {
    font-size: 15px;
}

.dd-item {
    display: flex;
    align-items: center;
    margin-top: 5px;
    padding-left: 7px;
}

.list-key {
    display: inline-flex;
    flex: 1;
}

.line-buttons {
    display: inline-flex;
    align-items: center;
}

.dropdown-content {
    position: absolute;
    top: 100%;
    z-index: 2;
    padding: 10px;
    margin-top: 5px;
    background-color: #282828;
}

/* Show the tooltip text when you mouse over the tooltip container */

.full-query {
    display: inline-flex;
    position: relative;
    border-bottom: 1px solid #606060;
    padding-bottom: 5px;
    flex: 3;
    margin-right: 10px;
}
</style>

<script>

export default {
  name: 'FavQueriesList',
  props: ['currentKeyspace', 'favQueries', 'toolTipShown'],
  methods: {
    loadFavQueries() {
      if (!(this.toolTipShown === 'favourites')) {
        this.$emit('toggle-tool-tip', 'favourites');
      } else {
        this.$emit('toggle-tool-tip');
      }
    },
    closeFavQueriesList() {
      this.$emit('toggle-tool-tip');
    },
    removeFavQuery(index, queryName) {
      this.$emit('remove-fav-query', queryName);
      this.favQueries.splice(index, 1);
    },
    typeFavQuery(query) {
      this.$emit('type-fav-query', query);
      this.$emit('toggle-tool-tip');
    },
  },
};
</script>
