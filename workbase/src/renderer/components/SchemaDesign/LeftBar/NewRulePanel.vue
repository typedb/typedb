<template>
  <div>
    <button class="btn define-btn" :class="(showPanel === 'rule') ? 'green-border': ''" @click="togglePanel">Rule</button>
    <div class="new-rule-panel-container" v-if="showPanel === 'rule'">
      <div class="title">
        Define New Rule
        <div class="close-container" @click="$emit('show-panel', undefined)"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
      </div>

      <div class="content">
        <div class="row">
          <input class="input-small label-input" v-model="ruleLabel" placeholder="Rule Label">
        </div>

        <div class="row">
          when
        </div>
        <div class="row">
          <textarea class="input rule-input" v-model="when"></textarea>
        </div>

        <div class="row">
          then
        </div>
        <div class="row">
          <textarea class="input rule-input" v-model="then"></textarea>
        </div>
    
        <div class="submit-row">
          <button class="btn submit-btn" @click="resetPanel">Clear</button>
          <loading-button v-on:clicked="defineRule" text="Submit" :loading="showSpinner" className="btn submit-btn"></loading-button>
        </div>

      </div>
    </div>
  </div>
</template>

<style scoped>

  .rule-input {
    min-width: 308px;
    max-width: 1000px;
    min-height: 100px;
    max-height: 300px;
  }

  .rule-input ::-webkit-scrollbar {
    width: 2px;
  }

  .rule-input ::-webkit-scrollbar-thumb {
    background: var(--green-4);
  }
  

  .close-container {
    position: absolute;
    right: 2px;
  }

  .submit-row {
    justify-content: space-between;
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: var(--container-padding);
  }


  .row {
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: var(--container-padding) var(--container-padding) 0px var(--container-padding);
    justify-content: space-between;
  }

  .new-rule-panel-container {
    position: absolute;
    left: 120px;
    top: 10px;
    background-color: var(--gray-2);
    border: var(--container-darkest-border);
  }

  .title {
    background-color: var(--gray-1); 
    display: flex;
    align-items: center;
    padding: var(--container-padding);
    border-bottom: var(--container-darkest-border);
  }

  .content {
    padding: var(--container-padding);
  }



</style>

<script>

  import logger from '@/../Logger';
  import { DEFINE_RULE } from '@/components/shared/StoresActions';
  import { createNamespacedHelpers } from 'vuex';

  export default {
    props: ['showPanel'],
    data() {
      return {
        showSpinner: false,
        ruleLabel: '',
        when: '',
        then: '',
      };
    },
    beforeCreate() {
      const { mapActions } = createNamespacedHelpers('schema-design');

      // methods
      this.$options.methods = {
        ...(this.$options.methods || {}),
        ...mapActions([DEFINE_RULE]),
      };
    },
    methods: {
      resetPanel() {
        this.ruleLabel = '';
        this.when = '';
        this.then = '';
      },
      defineRule() {
        if (this.ruleLabel === '' || this.when === '' || this.then === '') {
          this.$notifyError('Cannot define Rule with empty rule label or then statement or when statement');
        } else {
          this.showSpinner = true;

          this[DEFINE_RULE]({ ruleLabel: this.ruleLabel, when: `{${this.when}}`, then: `{${this.then}}` })
            .then(() => {
              this.showSpinner = false;
              this.$notifyInfo(`Rule, ${this.ruleLabel}, has been defined`);
              this.resetPanel();
            })
            .catch((e) => {
              logger.error(e.stack);
              this.showSpinner = false;
              this.$notifyError(e.message);
            });
        }
      },
      togglePanel() {
        if (this.showPanel === 'rule') this.$emit('show-panel', undefined);
        else this.$emit('show-panel', 'rule');
      },
    },
  };
</script>
