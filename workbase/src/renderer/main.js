import Vue from 'vue';
import VueRouter from 'vue-router';

import store from './store';

// UI Elements
import LoadingButton from './components/UIElements/LoadingButton.vue';
import VueTabs from './components/UIElements/VueTabs.vue';
import VueIcon from './components/UIElements/VueIcon.vue';
import VueSwitch from './components/UIElements/VueSwitch.vue';


// Modules
import { routes } from './routes';
import CustomPlugins from './customPlugins/';

Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

const SERVER_AUTHENTICATED = false;
const LANDING_PAGE = '/login';

// Disable devtools message
Vue.config.devtools = false;

// Register plugins
Vue.use(VueRouter);

// Add notification properties to Vue instance
CustomPlugins.registerNotifications();

// Register UIElements globally
Vue.component('loading-button', LoadingButton);
Vue.component('vue-tabs', VueTabs);
Vue.component('vue-icon', VueIcon);
Vue.component('vue-switch', VueSwitch);


// Define a Vue Router and map all the routes to components - as defined in the routes.js file.
const router = new VueRouter({
  linkActiveClass: 'active',
  routes,
});

// Set state variables in global store - this needs to happen before everything else
store.commit('setAuthentication', SERVER_AUTHENTICATED);
store.commit('setLandingPage', LANDING_PAGE);
store.commit('loadLocalCredentials', SERVER_AUTHENTICATED);

// Before loading a new route check if the user is authorised
router.beforeEach((to, from, next) => {
  if (to.path === '/login') next();
  if (store.getters.isAuthorised) next();
  else next('/login');
});

function initialiseStore() {
  // this.$store.dispatch('initGrakn');
  this.$router.push(LANDING_PAGE);
}

new Vue({
  router,
  store,
  created: initialiseStore,
}).$mount('#grakn-app');
