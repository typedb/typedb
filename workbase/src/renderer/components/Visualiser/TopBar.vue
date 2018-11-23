<template>
    <div class="top-bar-container">
        <div class="left">
            <router-link to="/design/schema"><button class="btn"><vue-icon icon="diagram-tree" className="vue-icon"></vue-icon></button></router-link>
            <img src="static/img/logo-text.png" class="grakn-icon">
        </div>

        <div class="center">
            <graql-editor :tabId="tabId" v-on:keyspace-not-selected="showKeyspaceToolTip = true"></graql-editor>
        </div>

        <div class="right">
            <keyspaces-handler :tabId="tabId" :showKeyspaceTooltip="showKeyspaceToolTip" v-on:keyspace-selected="showKeyspaceToolTip = false"></keyspaces-handler>
            <button class="btn" @click="$emit('toggle-preferences')"><vue-icon icon="cog" className="vue-icon"></vue-icon></button>
        </div>
    </div>
</template>

<style scoped>

    .top-bar-container {
        background-color: var(--gray-2);
        border-bottom: var(--container-darkest-border);
        width: 100%;
        height: 50px;
        position: relative;
        flex-direction: row;
        display: flex;
        justify-content: space-between;
        z-index: 2;
    }

    .left {
        padding: var(--container-padding);
        width: 140px;
        height: 100%;
        display: flex;
        align-items: center;
        vertical-align: middle;
        flex-direction: row;

        -webkit-touch-callout: none; /* iOS Safari */
        -webkit-user-select: none; /* Safari */
        -khtml-user-select: none; /* Konqueror HTML */
        -moz-user-select: none; /* Firefox */
        -ms-user-select: none; /* Internet Explorer/Edge */
        user-select: none; /* Non-prefixed version, currently
                                  supported by Chrome and Opera */
    }

    .grakn-icon {
        height: 25px;
        margin: 5px;
    }

    .center {
        padding: var(--container-padding);
        height: 100%;
        min-width: 800px;
        display: flex;
        flex-direction: row;
        align-items: center;
        justify-content: center;
        vertical-align: middle;
    }

    .right {
        padding: var(--container-padding);
        width: 140px;
        height: 100%;
        display: flex;
        justify-content: flex-end;
        align-items: center;
        }
</style>

<script>
    import KeyspacesHandler from '../shared/KeyspacesHandler.vue';
    import GraqlEditor from './TopBar/GraqlEditor/GraqlEditor.vue';

    export default {
      components: { KeyspacesHandler, GraqlEditor },
      props: ['tabId'],
      data() {
        return {
          showKeyspaceToolTip: false,
        };
      },
    };
</script>
