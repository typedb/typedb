// Import pages components
import MainTemplate from './components/MainTemplate/MainTemplate.vue';
import SchemaDesignContent from './components/SchemaDesign/SchemaDesignContent/SchemaDesignContent.vue';
import SettingsPage from './components/ManageSettings/SettingsPage.vue';
import KeyspacesPage from './components/ManageKeyspaces/KeyspacesPage.vue';
import DataManagementPage from './components/DataManagement/DataManagementContent/DataManagementContent.vue';
import LoginPage from './components/Login/LoginPage.vue';

export const isDataManagement = route => (route.path === '/develop/data');
export const isSchemaDesign = route => (route.path === '/design/schema');

// Routes
export const routes = [
  {
    path: '/',
    component: MainTemplate,
    children: [
      {
        path: '/design/schema',
        component: SchemaDesignContent,
      },
      {
        path: '/manage/settings',
        component: SettingsPage,
      },
      {
        path: '/manage/keyspaces',
        component: KeyspacesPage,
      },
      {
        path: '/develop/data',
        component: DataManagementPage,
      },
    ],
  }, {
    path: '/login',
    component: LoginPage,
  }];
