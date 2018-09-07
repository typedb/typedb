<template>
    <div class="top-bar-container">
        <div class="left">
            <img src="static/img/logo-text.png" class="grakn-icon">
            <!--<p class="title">VISUALISER</p>-->
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
        background-color: var(--gray-2);
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
        width: 140px;
        height: 100%;
        display: flex;
        align-items: center;
        vertical-align: middle;
        /*justify-content: space-between;*/
        flex-direction: row;

        -webkit-touch-callout: none; /* iOS Safari */
        -webkit-user-select: none; /* Safari */
        -khtml-user-select: none; /* Konqueror HTML */
        -moz-user-select: none; /* Firefox */
        -ms-user-select: none; /* Internet Explorer/Edge */
        user-select: none; /* Non-prefixed version, currently
                                  supported by Chrome and Opera */
    }

    .title {
        margin-left: 3px;
        font-size: 16px !important;
        font-weight: 400;
        letter-spacing: 1px;
        height: 25px;
        line-height: 25px;
    }

    .grakn-icon {
        height: 25px;
        margin: 5px;
        /*border: var(--container-darkest-border);*/
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
    import KeyspacesHandler from '../shared/KeyspacesHandler/KeyspacesHandler.vue';
    import GraqlEditor from './TopBar/GraqlEditor/GraqlEditor.vue';

    export default {
      props: ['localStore'],
      components: { KeyspacesHandler, GraqlEditor },
    };
</script>
