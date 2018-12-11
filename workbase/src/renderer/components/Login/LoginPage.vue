<template>
  <transition name="slide-fade" appear>
    <div class="wrapper">
      <div class="login-header">
        <img src="static/img/logo-text.png" class="icon">
        <div v-if="!isKgms" class="workbase">WORKBASE</div>
        <div v-else class="workbase2">WORKBASE FOR KGMS</div>
      </div>

      <div class="btn-row">
        <button v-if="!isKgms" @click="loginTocore" class="btn landing-btn">CORE</button>
        <button v-if="!isKgms" @click="isKgms = true" class="btn landing-btn">KGMS</button>
      </div>

      <div v-if="isKgms">
        <div class="login-panel z-depth-5">
            <!-- <div class="title">
              Server Address
            </div> -->


            <div class="content">
              <h1 class="label">Host:</h1>
              <input class="input left-input" v-model="serverHost">
              <h1 class="label">Port:</h1>
              <input class="input" type="number" v-model="serverPort">
            </div>

            <div class="content">
              <h1 class="label">Username:</h1>
              <input class="input left-input" v-model="username">
              <h1 class="label">Password:</h1>
              <input class="input" v-model="password" type="password">
            </div>

            <div class="content login-row">
              <loading-button v-on:clicked="loginToKgms" text="Log In" :loading="isLoading" className="btn login-btn"></loading-button>
            </div>

        </div>
      
      </div>


      
    </div>
  </transition>
</template>
<style scoped>

  .title {
      padding: var(--container-padding);
  }

  .content {
      padding: var(--container-padding);
      display: flex;
      align-items: center;
  }

  .login-row {
    display: flex;
    justify-content: flex-end;
  }

  .label {
    width: 52px;
    margin-right: 5px;
  }

  .left-input {
    margin-right: 5px;
  }

  .login-panel {
    border: var(--container-darkest-border);
    padding: var(--container-padding);
    display: flex;
    flex-direction: column;
    background-color: var(--gray-2);
  }

  .landing-btn {
    width: 200px;
    height: 100px;
    font-size: 50px !important;
  }

  .btn-row {
    margin-top: 50px;
    display: flex;
    flex-direction: row;
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

  .workbase {
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
import storage from '@/components/shared/PersistentStorage';
import Settings from '../ServerSettings';

export default {
  name: 'LoginPage',
  data() {
    return {
      isKgms: false,
      username: '',
      password: '',
      isLoading: false,
      serverHost: Settings.getServerHost(),
      serverPort: Settings.getServerPort(),
    };
  },
  watch: {
    serverHost(newVal) {
      Settings.setServerHost(newVal);
    },
    serverPort(newVal) {
      Settings.setServerPort(newVal);
    },
  },
  created() {
    window.addEventListener('keyup', (e) => {
      if (e.keyCode === 13 && !e.shiftKey && this.username.length && this.password.length) this.login();
    });
  },
  mounted() {
    this.$nextTick(() => {
      this.serverHost = Settings.getServerHost();
      this.serverPort = Settings.getServerPort();
    });
  },
  methods: {
    testConnection() {
      this.$store.dispatch('initGrakn', { username: this.username, password: this.password });
    },
    loginTocore() {
      this.$store.dispatch('initGrakn')
        .then(() => {
          this.$router.push('develop/data');
        })
        .catch((e) => {
          this.$notifyError(e, 'Login');
        });
    },
    loginToKgms() {
      this.isLoading = true;
      this.$store.dispatch('login', { username: this.username, password: this.password })
        .then(() => {
          this.isLoading = false;
          storage.set('user-credentials', JSON.stringify({ username: this.username, password: this.password }));
          this.$router.push('develop/data');
        })
        .catch((err) => {
          this.isLoading = false;
          let error;
          // TODO change this once gRPC errors fixed - we guess that credentials were wrong
          if (err.message.includes('2 UNKNOWN')) {
            error = 'Login failed. Check credentials and try again.';
          } else {
            error = err;
          }
          this.$notifyError(error, 'Login');
        });
    },
  },
};
</script>
