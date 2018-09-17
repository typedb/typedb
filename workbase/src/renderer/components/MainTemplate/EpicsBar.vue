<template>
    <nav role="navigation" class="navbar-fixed noselect z-depth-1-half">
        <div class="nav-wrapper">
            <ul class="menu-list">
                <!-- <li class="first-level" >DESIGN</li>
                    <router-link tag="li" to="/design/schema" class="sub-menu"><a>Schema</a></router-link> -->
                    <!-- <li class="sub-menu"><a>Change Management</a></li>
                    <li class="sub-menu"><a>KB Documentation</a></li> -->
                <li class="first-level noselect">DATA</li>
                    <!-- <li class="sub-menu"><a>Query Visualisation</a></li> -->
                    <router-link tag="li" to="/develop/data" class="sub-menu"><a>visualiser</a></router-link>
                <!-- <li class="first-level" >MIGRATION</li>
                    <li class="sub-menu"><a>Scripts</a></li>
                    <li class="sub-menu"><a>Migrators</a></li>
                <li class="first-level">PERFORMANCE</li>
                    <li class="sub-menu"><a>Dashboard</a></li>
                    <li class="sub-menu"><a>Reports</a></li> -->
                <li class="first-level">MANAGE</li>
                    <!-- <li class="sub-menu">Users</li> -->
                    <router-link tag="li" to="/manage/keyspaces" class="sub-menu" id="manage-keyspaces"><a>keyspaces</a></router-link>
                    <!-- <li class="sub-menu">Server</li> -->
                    <router-link tag="li" to="/manage/settings" class="sub-menu" id="settings"><a>settings</a></router-link>
                <li class="first-level" v-if="isUserLogged">USER</li>
                    <li class="sub-menu" @click="logout" v-if="isUserLogged"><a>Logout</a></li>
            </ul>
        </div>
    </nav>
</template>

<style scoped>
.nav-wrapper {
  padding-top: 5px;
  height: 100%;
}

.first-level {
  margin-top: 25px;
  font-size: 80%;
  opacity: 0.6;
}

.menu-list {
  display: flex;
  flex-direction: column;
}

li {
  margin: 4px 10px;
}

.navbar-fixed {
  position: relative;
  z-index: 10;
  min-height: 22px;
  height: 100%;
}

.dark .navbar-fixed {
  background-color: #282828;
}

.light .navbar-fixed {
  background-color: #f2f4f7;
}

.active a {
  border-bottom: 2px solid #00eca2;
}
</style>

<script>
export default {
  data() {
    return {
      isUserLogged: false,
    };
  },
  mounted() {
    this.$nextTick(() => {
      this.isUserLogged = this.$store.getters.userLogged;
    });
  },
  methods: {
    logout() {
      this.$store.dispatch('logout');
      this.$router.push('/login');
    },
  },
};
</script>
