import Grakn from 'grakn';
import EngineSettings from '@/components/EngineSettings';

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


// TODO: for now if we import in a keyspace that is currently selected in some other page the user
// will need to manually refresh to see imported data
// create a 'refreshKeyspace' field that other stores can listen on.
// export const importFromFile = (state, payload) => EngineClient.importFromFile(payload);

export const login = (context, credentials) =>
  // TODO: Keyspace 'grakn' is hardcoded until we will implement an authenticate endpoint in gRPC
  context.dispatch('initGrakn', credentials).then(() => {
    context.commit('setCredentials', credentials);
    context.commit('userLogged', true);
  })
;

export const initGrakn = (context, credentials) => {
  const grakn = new Grakn(EngineSettings.getEngineGrpcUri(), credentials);
  context.commit('setGrakn', grakn);
  context.dispatch('loadKeyspaces');
};

export const logout = (context) => {
  context.commit('deleteCredentials');
  context.commit('userLogged', false);
  // Need to notify all the other states that they need to invalidate GraknClient
};

export const closeSession = (context) => {
  context.state.grakn.session.close();
};
