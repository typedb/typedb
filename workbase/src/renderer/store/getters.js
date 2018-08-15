export const grakn = state => state.grakn;
export const allKeyspaces = state => state.keyspaces;
export const isAuthorised = state => (!state.isAuthenticated || state.credentials);
export const landingPage = state => state.landingPage;
export const userLogged = state => state.userLogged;
export const credentials = state => state.credentials;
export const isGraknRunning = state => state.isGraknRunning;
