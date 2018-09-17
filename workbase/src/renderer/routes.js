// Import pages components
import MainTemplate from './components/MainTemplate/MainTemplate.vue';
import SchemaDesignContent from './components/SchemaDesign/SchemaDesignContent/SchemaDesignContent.vue';
import VisualiserPage from './components/Visualiser/VisualiserContent.vue';
import LoginPage from './components/Login/LoginPage.vue';

export const isVisualiser = route => (route.path === '/develop/data');
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
        path: '/develop/data',
        component: VisualiserPage,
      },
    ],
  }, {
    path: '/login',
    component: LoginPage,
  }];
