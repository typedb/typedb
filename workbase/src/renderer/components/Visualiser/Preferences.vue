<template>
    <div class="preferences-container z-depth-5">
        <div class="preferences-header">
            <div class="preferences-title">Preferences</div>
            <div class="close-container" @click="$emit('close-preferences')"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
        </div>
        <div class="content">
            <div class="left">
                <div :class="(currentPreference === 'keyspaces') ? 'item item-selected' : 'item'">
                    <vue-icon icon="chevron-right" className="vue-icon" iconSize="14"></vue-icon>
                    <div class="label">Keyspaces</div>
                </div>
                <div class="item">
                    <vue-icon icon="chevron-right" className="vue-icon" iconSize="14"></vue-icon>
                    <div class="label">Connection</div>
                </div>
            </div>
            <div class="right">
                <keyspace-manager></keyspace-manager>
            </div>
        </div>

    </div>
</template>
<style scoped>

    .preferences-container {
        position: absolute;
        width: 800px;
        height: 500px;
        background-color: var(--gray-2);
        top: 20%;
        right: 24.65%;
        z-index: 1;
        border: var(--container-darkest-border);
        display: flex;
        flex-direction: column;
    }

    .preferences-header {
        height: 22px;
        background-color: var(--gray-1);
        display: flex;
        justify-content: center;
        align-items: center;
        border-bottom: var(--container-darkest-border);
    }

    .close-container {
        position: absolute;
        right: 2px;
    }

    .content {
        display: flex;
        flex-direction: row;
        width: 100%;
        height: 100%;
    }

    .left {
        border-right: var(--container-darkest-border);
        width: 100px;
    }

    .item {
        display: flex;
        align-items: center;
        padding: var(--container-padding);
        height: 22px;
        cursor: pointer;
    }

    .item-selected {
        background-color: var(--purple-3);
    }

    .right {
        width: 100%;
    }


</style>

<script>
import KeyspaceManager from './KeyspaceManager';

export default {
  name: 'Preferences',
  components: { KeyspaceManager },
  data() {
    return {
      currentPreference: 'keyspaces',
    };
  },
};
</script>
