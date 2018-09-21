import storage from '@/components/shared/PersistentStorage';


function getPreferencesMap() {
  let loadedMap = storage.get('keyspaces-preferences');

  if (loadedMap) {
    loadedMap = JSON.parse(loadedMap);
  } else {
    loadedMap = {};
    storage.set('keyspaces-preferences', '{}');
  }
  return loadedMap;
}

function emptyKeyspaceMap() {
  return {
    label: {},
    colour: {},
    position: {},
  };
}

// Getter and setter for preferences map

function getMap(keyspace) {
  const prefMap = getPreferencesMap();
  if (keyspace in prefMap) {
    return prefMap[keyspace];
  }
  // If current keyspace does not have preferences map create one
  prefMap[keyspace] = emptyKeyspaceMap();
  storage.set('keyspaces-preferences', JSON.stringify(prefMap));
  return prefMap[keyspace];
}


function flushMap(keyspace, map) {
  const fullMap = JSON.parse(storage.get('keyspaces-preferences'));
  fullMap[keyspace] = map;
  storage.set('keyspaces-preferences', JSON.stringify(fullMap));
}

// Labels on types

function getTypeLabels(type) {
  const keyspace = storage.get('current_keyspace_data');
  const map = getMap(keyspace);
  return map.label[type] || [];
}


function toggleLabelByType({ type, attribute }) {
  const keyspace = storage.get('current_keyspace_data');
  const map = getMap(keyspace);

  // Create map for current type if it does not exist
  if (!map.label[type]) {
    map.label[type] = [];
  }
  // If map includes current type - remove it
  if (map.label[type].includes(attribute)) {
    map.label[type].splice(map.label[type].indexOf(attribute), 1);
  } else if (attribute === undefined) { // attribute is undefined when we reset the labels
    map.label[type] = [];
  } else { // If map does not include current type - add it
    map.label[type].push(attribute);
  }
  flushMap(keyspace, map);
}

function getTypeColours(type) {
  const keyspace = storage.get('current_keyspace_data');
  const map = getMap(keyspace);
  return map.colour[type] || [];
}

function toggleColourByType({ type, colourString }) {
  const keyspace = storage.get('current_keyspace_data');
  const map = getMap(keyspace);

  // Create map for current type if it does not exist
  if (!map.colour[type]) {
    map.colour[type] = [];
  }
  // If map includes current type - remove it
  if (map.colour[type].includes(colourString)) {
    map.colour[type] = '';
  } else if (map.colour[type].length) {
    map.colour[type] = '';
    map.colour[type] = colourString;
  } else { // If map does not include current type - add it
    map.colour[type] = colourString;
  }
  flushMap(keyspace, map);
}


export default {
  getTypeLabels,
  toggleLabelByType,
  getTypeColours,
  toggleColourByType,
};

