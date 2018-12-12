<template>
  <transition name="slide-fade" appear>
    <div class="wrapper noselect">
       <div @click="isKgms = false"><vue-icon v-if="isKgms" icon="arrow-left" class="vue-icon back-arrow" iconSize="30"></vue-icon></div>



      <div class="login-header">
        <img src="static/img/logo-text.png" class="icon">
        <div v-if="!isKgms" class="workbase">WORKBASE</div>
        <div v-else class="workbase2">WORKBASE FOR KGMS</div>
      </div>

      <div class="btn-row">
        <button v-if="!isKgms" @click="loginTocore" class="btn landing-btn">CORE</button>
        <button v-if="!isKgms" @click="isKgms = true" class="btn landing-btn">KGMS</button>
      </div>

      <div class="login-panel z-depth-5">
            <div class="header">
              Server Address
            </div>
            <div class="content">
              <h1 class="label">Host:</h1>
              <input class="input left-input" v-model="serverHost">
              <h1 class="label">Port:</h1>
              <input class="input" type="number" v-model="serverPort">
            </div>
        </div>

      <div v-if="isKgms">
        <div class="login-panel z-depth-5">
            <div class="header">
              Log In
            </div>
            <div class="content">
              <h1 class="label">Username:</h1>
              <input class="input left-input" v-model="username">
              <h1 class="label">Password:</h1>
              <input class="input" v-model="password" type="password">
              <div class="login-row">
              <loading-button v-on:clicked="loginToKgms" text="Log In" :loading="isLoading" className="btn login-btn"></loading-button>
              </div>
            </div>



        </div>
      
      </div>


      
    </div>
  </transition>
</template>
<style scoped>

.back-arrow {
  position: absolute;
  left: 10px;
  top: 10px;
  cursor: pointer;
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
}

  .title {
      padding: var(--container-padding);
  }

  .content {
      padding: var(--container-padding);
      display: flex;
      align-items: center;
  }

  .login-row {
    margin-left: 5px;
  }


  .left-input {
    margin-right: 5px;
  }

  .login-panel {
    margin-top: 50px;
    border: var(--container-darkest-border);
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
import Grakn from 'grakn';
import storage from '@/components/shared/PersistentStorage';
import ServerSettings from '@/components/ServerSettings';

export default {
  name: 'LoginPage',
  data() {
    return {
      isKgms: false,
      username: '',
      password: '',
      isLoading: false,
      serverHost: ServerSettings.getServerHost(),
      serverPort: ServerSettings.getServerPort(),
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
    loginTocore() {
      const grakn = new Grakn(ServerSettings.getServerUri(), { username: this.username, password: this.password });
      grakn.session('grakn').transaction().then(() => {
        this.$router.push('develop/data');
      })
        .catch((e) => {
          let error;
          if (e.message.includes('2 UNKNOWN')) {
            error = 'Login failed. Check if Grakn Core is running.';
          } else {
            error = e;
          }
          this.$notifyError(error);
        });
    },
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
            error = 'Login failed. <br> - make sure Grakn KGMS is running <br> - check if credentials are correct';
          } else {
            error = e;
          }
          this.$notifyError(error);
        });
    },
  },
};
</script>
