<template>
    <div class="error-container">
        <div class="close-container">
            <div @click="$emit('close-error')"><vue-icon icon="cross" iconSize="15" className="tab-icon"></vue-icon></div>
            <div @click="copyError"><vue-icon icon="duplicate" iconSize="15" className="tab-icon"></vue-icon></div>
        </div>
        <div class="header">ERROR MESSAGE</div>
        <div>{{errorMsg}}</div>
    </div>
</template>

<style scoped>

    .close-container{
        position: absolute;
        right: 0px;
        top:0px;
        align-items: center;
        flex-direction: column;
        display: flex;
        float: right;
    }

    .error-container {
        background-color: var(--error-container-color);
        padding: var(--container-padding);
        border: var(--container-darkest-border);
        width: 100%;
        max-height: 140px;
        margin-top: 20px;
        overflow: auto;
        position: relative;
        white-space: pre-line;
        word-wrap: break-word;
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
        // Create a dummy input to copy the string array inside it
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
      },
    },
  };
</script>
