<template>
  <transition name="slide-down" appear>
    <nav role="navigation" class="navbar-fixed noselect">
        <div class="nav-wrapper">
            <div class="left">
                <span  @click="toggleNewTypePanel" class="menu-item" :class="{'disabled':!isActive}"><img src="static/img/icons/icon_add_white.svg" class="img-button"></span>
                <span  @click="toggleManageAttributesPanel" class="manage-btn" :class="{'disabled':!isActive}">Attributes</span>
                <span  @click="toggleManageRolesPanel" class="manage-btn" :class="{'disabled':!isActive}">Roles</span>
            </div>
            <div class="right">
                <div class="line">
                    <!-- <input class="grakn-input search" placeholder="search for nodes"> -->
                </div>
                <keyspaces-handler :localStore="localStore" :toolTipShown="toolTipShown" v-on:toggle-tool-tip="toggleToolTip">></keyspaces-handler>
            </div>
        </div>
    </nav>
  </transition>
</template>

<style scoped>

.manage-btn{
  font-size: 90%;
  background-color: #3E3E3F;
  outline: none;
  box-shadow: none;
  padding: 3px 5px;
  border-radius: 3px;
  cursor: pointer;
  opacity: 0.9;
  margin: 0 8px;
}

.menu-item{
    cursor: pointer;
}

.disabled{
    opacity:0.5;
    cursor: default;
}

.line{
    display: flex;
    flex-direction: row;
}

.search{
    margin-right: 8px;
}

.img-button{
    margin: 0 8px;
}


.nav-wrapper {
    display: flex;
    padding: 5px;
    align-items: center;
    border-bottom: 1px solid #29292B;
}

.right{
    display: flex;
    justify-content: flex-end;
    align-items: center;
    flex: 1;
}

.left{
    display: flex;
    justify-content: flex-start;
    align-items: center;
    flex: 1;
}

.navbar-fixed {
    position: relative;
    z-index: 2;
    min-height: 22px;
    width: 100%;
}

.dark .navbar-fixed{
    background-color: #1A1A1A;
}

.light .navbar-fixed{
    background-color:#f2f4f7;
}

</style>

<script>
import KeyspacesHandler from '../../shared/KeyspacesHandler/KeyspacesHandler.vue';

export default {
  name: 'MenuBar',
  components: { KeyspacesHandler },
  props: ['localStore'],
  data() {
    return {
      toolTipShown: undefined,
    };
  },
  computed: {
    isActive() { return this.localStore.isActive(); },
  },
  methods: {
    toggleNewTypePanel() {
      if (this.isActive) this.$emit('toggle-new-type-panel');
    },
    toggleManageRolesPanel() {
      if (this.isActive) this.$emit('toggle-roles-panel');
    },
    toggleManageAttributesPanel() {
      if (this.isActive) this.$emit('toggle-attributes-panel');
    },
    toggleToolTip(val) {
      if (this.toolTipShown === val) {
        this.toolTipShown = undefined;
      } else {
        this.toolTipShown = val;
      }
    },
  },
};
</script>
