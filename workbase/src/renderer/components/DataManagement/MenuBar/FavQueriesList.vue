<template>
<div>
    <button @click="loadFavQueries" class="btn btn-default" :class="{'disabled':!currentKeyspace, 'btn-selected':toolTipShown === 'favourites'}" id="fav-queries-btn">
        <i v-bind:class="{ fas: toolTipShown === 'favourites' }" class="far fa-star"></i>
    </button>
    <transition name="slide-fade">
        <div v-if="toolTipShown === 'favourites'" class="dropdown-content" id="fav-queries-list">
            <div class="panel-heading">
                <div></div>
                <h4><i class="page-header-icon far fa-star"></i>Saved queries</h4>
                <a @click="closeFavQueriesList"><i class="fas fa-times"></i></a>
            </div>
            <div class="panel-body" v-if="favQueries.length">
                <div class="dd-item" v-for="(query,index) in favQueries" :key="index">
                    <div class="full-query">
                        <span class="list-key" id="list-key"> {{query.name}}</span>
                    </div>
                    <div class="line-buttons">
                        <button id='use-btn' class="btn bold" @click="typeFavQuery(query.value)">USE</button>
                        <button id='delete-btn' class="btn" @click="removeFavQuery(index, query.name)"><i class="fas fa-trash-alt"></i></button>
                    </div>
                </div>
            </div>
            <div class="panel-body" v-else>
                <div class="dd-item">
                    <div class="no-saved" id="no-saved">
                        No saved queries.
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


a {
    margin-left: auto;
}

.fa-times{
  cursor: pointer;
  padding-left: 10px;
}

.panel-heading {
    padding: 5px 10px;
    display: flex;
    justify-content: space-between;
    align-items: center;
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
