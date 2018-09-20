<template>
    <div class="error-container z-depth-3 noselect">
        <div class="column">
            <div class="header">ERROR</div>
            <div>{{errorMsg}}</div>
        </div>
        <div class="editor-tab">
            <div @click="$emit('close-error')"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
            <div @click="copyError"><vue-icon icon="clipboard" iconSize="12" className="tab-icon"></vue-icon></div>
        </div>
    </div>
</template>

<style scoped>

    .column {
        display: flex;
        flex-direction: column;
        width: 100%;
        overflow-y: auto;
        padding: var(--container-padding);
    }

    .column::-webkit-scrollbar {
        width: 1px;
    }

    .column::-webkit-scrollbar-thumb {
        background: var(--green-4);
    }

    .editor-tab {
        max-height: 140px;
        width: 13px;
        flex-direction: column;
        display: flex;
        position: relative;
        float: right;
        border-left: 1px solid var(--red-4);
    }

    .error-container {
        background-color: var(--error-container-color);
        border: var(--container-darkest-border);
        width: 100%;
        max-height: 140px;
        margin-top: 10px;
        position: relative;
        white-space: pre-line;
        word-wrap: break-word;
        display: flex;
        flex-direction: row;
    }

    .header {
        display: flex;
        align-items: center;
        justify-content: center;
        margin-bottom: 10px;
    }
</style>

<script>

  export default {
    name: 'ErrorContainer',
    props: ['errorMsg'],
    methods: {
      copyError() {
        // Create a dummy queryNameInput to copy the string array inside it
        const dummyInput = document.createElement('input');

        // Add it to the document
        document.body.appendChild(dummyInput);

        // Set its ID
        dummyInput.setAttribute('id', 'dummy_id');

        // Output the array into it
        document.getElementById('dummy_id').value = this.errorMsg;

        // Select it
        dummyInput.select();

        // Copy its contents
        document.execCommand('copy');

        // Remove it as its not needed anymore
        document.body.removeChild(dummyInput);

        this.$notifyInfo('Error message copied.');
      },
    },
  };
</script>
