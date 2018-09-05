<template>
    <div class="top-bar-container">
        <div class="left">
            <img src="static/img/logo-text.png" class="grakn-icon">
            <h1>visualiser</h1>
        </div>

        <div class="center">
            <graql-editor :localStore="localStore"></graql-editor>
        </div>

        <div class="right">
            <keyspaces-handler :localStore="localStore"></keyspaces-handler>
        </div>
    </div>
</template>

<style scoped>

    .top-bar-container {
        background-color: var(--gray-1);
        border-bottom: var(--container-darkest-border);
        width: 100%;
        height: 50px;
        position: relative;
        flex-direction: row;
        display: flex;
        justify-content: space-between;
        z-index: 1;
    }

    .left {
        padding: var(--container-padding);
        width: 180px;
        height: 100%;
        display: flex;
        align-items: center;
        vertical-align: middle;
        justify-content: space-between;
    }

    .grakn-icon {
        height: 25px;
        margin-right: 5px;
    }

    .center {
        padding: var(--container-padding);
        height: 100%;
        min-width: 700px;
        display: flex;
        flex-direction: row;
        align-items: center;
        justify-content: center;
        vertical-align: middle;
    }

    .right {
        padding: var(--container-padding);
        width: 180px;
        height: 100%;
        display: flex;
        justify-content: flex-end;
        align-items: center;
        }
</style>

<script>
    import KeyspacesHandler from '../shared/KeyspacesHandler/KeyspacesHandler.vue';
    import GraqlEditor from './TopBar/GraqlEditor/GraqlEditor.vue';

    export default {
      props: ['localStore'],
      components: { KeyspacesHandler, GraqlEditor },
    };
</script>
