<template>
  <transition name="slide-fade" appear>
    <div class="wrapper">
      <div class="login-header">
        <img src="static/img/logo-text.png" class="icon">
        <div class="workbase">WORKBASE</div>
      </div>
      <div class="inner z-depth-5">
        <div class="title">Login</div>
          <div class="list">
          <div class="line">
            <div class="label">Username</div>
            <div><input class="grakn-input" v-model="username"></div>
          </div>
          <div class="line">
            <div class="label">Password</div>
            <div><input class="grakn-input" v-model="password" type="password"></div>
          </div>
          <div class="line align-right">
              <loading-button :clickFunction="login" value="Login" :isLoading="isLoading" :disabled="!username.length || !password.length || isLoading"></loading-button>
            </div>
        </div>
        <div>
        </div>
      </div>
      <div class="inner settings z-depth-5">
        <div class="title">Grakn Server</div>
        <engine-tab></engine-tab>
      </div>
    </div>
  </transition>
</template>
<style scoped>

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
  margin-left: 40%;
}

.wrapper{
  display: flex;
  flex-direction: column;
  align-items: center;
  padding-top:30px;
}

.inner {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  margin-top: 50px;
  background-color: #202020;
  width: 400px;
  height: 200px;
}

.settings {
  height: 160px;
}

.title{
  display: flex;
  justify-content: center;
  font-size:120%;
  font-weight: bold;
  margin:20px 0px;
}

.list {
    display: flex;
    flex-direction: column;
 }

 .line {
    padding: 4px;
    display: flex;
    align-items: center;
}
.label{
  margin: 0 5px;
}

.align-right{
  margin-left: auto;
}

</style>
<script>
import storage from '@/components/shared/PersistentStorage';

export default {
  name: 'LoginPage',
  data() {
    return {
      username: '',
      password: '',
      isLoading: false,
    };
  },
  created() {
    window.addEventListener('keyup', (e) => {
      if (e.keyCode === 13 && !e.shiftKey && this.username.length && this.password.length) this.login();
    });
  },
  methods: {
    login() {
      this.isLoading = true;
      this.$store.dispatch('login', { username: this.username, password: this.password })
        .then(() => {
          this.isLoading = false;
          storage.set('user-credentials', JSON.stringify({ username: this.username, password: this.password }));
          this.$router.push(this.$store.getters.landingPage);
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
