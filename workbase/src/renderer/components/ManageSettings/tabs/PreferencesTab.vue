<template>
  <div clas="pref-wrapper">
    <div class ="clear-pref-btn">
      <button id="clear-preferences" class="btn" @click="clearPreferences">Clear Preferences</button>
    </div>
  </div>
</template>

<style scoped>

.pref-wrapper {
  display: flex;
  align-items: center;
  justify-content: center;
}

.clear-pref-btn{
  display: flex;
  justify-content: center;
}

</style>

<script>
  import storage from '@/components/shared/PersistentStorage';

  export default {
    name: 'PreferencesTab',

    methods: {
      clearPreferences() {
        this.$notifyConfirmDelete('Confirm clearing of preferences', () => { storage.clear(); });
      },
    },
  };
</script>
