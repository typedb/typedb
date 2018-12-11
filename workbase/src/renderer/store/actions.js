import Grakn from 'grakn';
import ServerSettings from '@/components/ServerSettings';

export const loadKeyspaces = async (context) => {
  try {
    const resp = await context.state.grakn.keyspaces().retrieve();
    context.commit('setIsGraknRunning', true);
    context.commit('setKeyspaces', resp);
  } catch (e) {
    context.commit('setIsGraknRunning', false);
  }
};

export const createKeyspace = (context, name) => context.state.grakn.session(name)
  .transaction(Grakn.txType.WRITE)
  .then(() => { context.dispatch('loadKeyspaces'); });

export const deleteKeyspace = async (context, name) => context.state.grakn.keyspaces().delete(name)
  .then(() => { context.dispatch('loadKeyspaces'); });

export const login = (context, credentials) =>
  context.dispatch('initGrakn', credentials).then(() => {
    context.commit('setCredentials', credentials);
    context.commit('userLogged', true);
  });

export const initGrakn = (context, credentials) => {
  try {
    const grakn = new Grakn(ServerSettings.getServerUri(), credentials);
    grakn.session('grakn').transaction();

    context.commit('setGrakn', grakn);
    context.dispatch('loadKeyspaces');
  } catch (e) {
    console.log(e);
  }
};

export const logout = (context) => {
  context.commit('deleteCredentials');
  context.commit('userLogged', false);
  // Need to notify all the other states that they need to invalidate GraknClient
};

export const closeSession = (context) => {
  context.state.grakn.session.close();
};
