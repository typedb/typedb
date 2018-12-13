<template>
  <transition name="slide-fade" appear>
    <div class="wrapper noselect" v-if="showLoginPage">
      <div class="login-header">
        <img src="static/img/logo-text.png" class="icon">
        <div v-if="!showPanel" class="workbase3">WORKBASE</div>
        <div v-else class="workbase2">WORKBASE FOR KGMS</div>
      </div>

      <div class="login-panel" v-if="showPanel">
        <div class="header">
          Log In
        </div>
        <div class="row">
          <div class="column">
            <div class="row">
              <h1 class="label">Host:</h1>
              <input class="input left-input" v-model="serverHost">
            </div>
            <div class="row">
              <h1 class="label">Username:</h1>
              <input class="input left-input" v-model="username">
            </div>
            <div class="row">
              <button @click="isKgms = !isKgms" class="btn landing-btn non-btn"></button>
            </div>
          </div>
          <div class="column">
            <div class="row">
              <h1 class="label">Port:</h1>
              <input class="input" type="number" v-model="serverPort">
            </div>
            <div class="row">
              <h1 class="label">Password:</h1>
              <input class="input" v-model="password">
            </div>
            <div class="row flex-end">
              <loading-button v-on:clicked="loginToKgms()" text="Login" :loading="isLoading" className="btn login-btn"></loading-button>
            </div>
          </div>
        </div>
      </div>
      
    </div>
  </transition>
</template>
<style scoped>
  .arrow-left {
    padding-right: 2px;
  }

  .flex-end {
    justify-content: flex-end;
  }

  .non-btn {
    background-color: var(--gray-2) !important;
    border: 0px;
    padding-left: 0px;
  }

  .input {
    width: 100%;
  }

  .column {
    display: flex;
    flex-direction: column;
    width: 100%;
  }

  .row {
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: var(--container-padding);
  }

  .header {
    background-color: var(--gray-1);
    height: 22px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-bottom: var(--container-darkest-border);
  }

  .label {
    margin-right: 5px;
    width: 85px;
  }

  .login-panel {
    margin-top: 50px;
    border: var(--container-darkest-border);
    display: flex;
    flex-direction: column;
    background-color: var(--gray-2);
    width: 384px;
  }

  .btn-row {
    display: flex;
    flex-direction: row;
    width: 100%;
  }

  .slide-fade-enter-active {
      transition: all 1s ease;
  }
  .slide-fade-enter,
  .slide-fade-leave-active {
      opacity: 0;
  }

  .icon {
    width: 250px;
    margin-top: 50px;
  }

  .login-header {
    display: flex;
    flex-direction: column;
    align-items: center;
    width: 400px;
  }

   .workbase3 {
    right: 30%;
    font-size: 150%;
    color: #00eca2;
    margin-left: 170px;
  }


  .workbase2 {
    right: 30%;
    font-size: 150%;
    color: #00eca2;
    margin-left: 90px;
  }

  .wrapper{
    display: flex;
    flex-direction: column;
    align-items: center;
    padding-top: 30px;
  }

</style>
<script>
import Grakn from 'grakn';
import storage from '@/components/shared/PersistentStorage';
import ServerSettings from '@/components/ServerSettings';

export default {
  name: 'LoginPage',
  data() {
    return {
      username: '',
      password: '',
      isLoading: false,
      serverHost: ServerSettings.getServerHost(),
      serverPort: ServerSettings.getServerPort(),
      showPanel: true,
      showLoginPage: false,
    };
  },
  watch: {
    serverHost(newVal) {
      ServerSettings.setServerHost(newVal);
    },
    serverPort(newVal) {
      ServerSettings.setServerPort(newVal);
    },
  },
  beforeCreate() {
    const grakn = new Grakn(ServerSettings.getServerUri(), { username: this.username, password: this.password });
    grakn.session('grakn').transaction().then(() => {
      this.$router.push('develop/data');
      this.$store.dispatch('initGrakn');
    })
      .catch((e) => {
        if (!e.message.includes('2 UNKNOWN')) {
          this.showPanel = false;
          this.$notifyError('Looks like Grakn is not running: <br> - Please make sure Grakn is running and refresh workbase.');
        }
        this.showLoginPage = true;
      });
  },
  created() {
    window.addEventListener('keyup', (e) => {
      if (e.keyCode === 13 && !e.shiftKey && this.username.length && this.password.length) this.loginToKgms();
    });
  },
  mounted() {
    this.$nextTick(() => {
      this.serverHost = ServerSettings.getServerHost();
      this.serverPort = ServerSettings.getServerPort();
    });
  },
  methods: {
    loginToKgms() {
      this.isLoading = true;
      const grakn = new Grakn(ServerSettings.getServerUri(), { username: this.username, password: this.password });
      grakn.session('grakn').transaction().then(() => {
        this.$store.dispatch('login', { username: this.username, password: this.password });
        storage.set('user-credentials', JSON.stringify({ username: this.username, password: this.password }));
        this.isLoading = false;
        this.$router.push('develop/data');
      })
        .catch((e) => {
          this.isLoading = false;
          let error;
          if (e.message.includes('2 UNKNOWN') || e.message.includes('14 UNAVAILABLE')) {
            error = 'Login failed: <br> - make sure Grakn KGMS is running <br> - check that host and port are correct <br> - check if credentials are correct';
          } else {
            error = e;
          }
          this.$notifyError(error);
        });
    },
  },
};
</script>
