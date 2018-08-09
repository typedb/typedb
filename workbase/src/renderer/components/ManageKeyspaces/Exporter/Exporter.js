/* eslint no-underscore-dangle: 0 */

// import EngineClient from '../../EngineClient';

// const fs = require('fs');
// const DEFINE_CHUNK = 'define \n\n';

// function loadEntities(keyspace) {
//   return new Promise((resolve) => {
//     EngineClient.graqlQuery({ query: 'match $x sub entity; get;', keyspace })
//       .then((resp) => {
//         const entities = [];
//         const nodes = JSON.parse(resp).map(e => e.x);
//         const promises = nodes
//           .filter(x => x.label !== 'entity')
//           .map(async (entity) => {
//             await entity.loadAttributesAndSave();
//             await entity.loadPlaysAndSave();
//             entities.push(entity);
//           });
//         Promise.all(promises).then(() => resolve(entities));
//       });
//   });
// }

// function loadAttributes(keyspace) {
//   return new Promise((resolve) => {
//     EngineClient.graqlQuery({ query: 'match $x sub attribute; get;', keyspace })
//       .then((resp) => {
//         const nodes = JSON.parse(resp).map(e => e.x);
//         const attributes = nodes
//           .filter(x => x.label !== 'attribute');
//         resolve(attributes);
//       });
//   });
// }

// function loadRoles(keyspace) {
//   return new Promise((resolve) => {
//     EngineClient.graqlQuery({ query: 'match $x sub role; get;', keyspace })
//       .then((resp) => {
//         const nodes = JSON.parse(resp).map(e => e.x);
//         const roles = nodes
//           .filter(x => x.label !== 'role')
//           .filter(x => !x.implicit)
//           .map(role => role.label);
//         resolve(roles);
//       });
//   });
// }

// function loadRelationships(keyspace) {
//   return new Promise((resolve) => {
//     EngineClient.graqlQuery({ query: 'match $x sub relationship; get;', keyspace })
//       .then((resp) => {
//         const relationships = [];
//         const nodes = JSON.parse(resp).map(e => e.x);
//         const promises = nodes
//           .filter(x => !x.implicit)
//           .filter(x => x.label !== 'relationship')
//           .map(async (relationship) => {
//             await relationship.loadAttributesAndSave();
//             await relationship.loadPlaysAndSave();
//             await relationship.loadRelatesAndSave();
//             relationships.push(relationship);
//           });
//         Promise.all(promises).then(() => resolve(relationships));
//       });
//   });
// }


// // Serialisers

// function serialiseEntities(entities) {
//   let serialised = '# Entities \n\n';
//   entities.forEach((entity) => {
//     serialised += `"${entity.label}" sub "${entity.super}"`;
//     entity.plays.filter(x => !x.implicit).map(x => x.label).sort().forEach((role) => { serialised += `\n\tplays "${role}"`; });
//     if (entity.attributes.length > 0) serialised += '\n';
//     entity.attributes.map(x => x.label).sort().forEach((attr) => { serialised += `\n\thas "${attr}"`; });
//     serialised += ';\n\n';
//   });
//   serialised += '\n\n';
//   return serialised;
// }

// function serialiseAttributes(attributes) {
//   let serialised = '# Attributes \n\n';
//   attributes.forEach((attribute) => { serialised += `"${attribute.label}" sub "${attribute.super}" datatype ${attribute.dataType.split('.').pop().toLowerCase()};\n`; });
//   serialised += '\n\n';
//   return serialised;
// }

// function serialiseRelationships(relationships) {
//   let serialised = '# Relationships \n\n';
//   relationships.forEach((rel) => {
//     serialised += `"${rel.label}" sub "${rel.super}"`;
//     rel.relates.map(x => x.label).sort().forEach((role) => { serialised += `\n\trelates "${role}"`; });
//     rel.attributes.map(x => x.label).sort().forEach((attr) => { serialised += `\n\thas "${attr}"`; });
//     serialised += ';\n\n';
//   });

//   serialised += '\n\n';

//   return serialised;
// }

// function serialiseRoles(roles) {
//   let serialised = '# Roles\n\n';
//   roles.forEach((role) => { serialised += `"${role}" sub role;\n`; });
//   return serialised;
// }


// function writeToFile(chunks, path) {
//   fs.writeFileSync(path, chunks);
// }


// async function exportKeyspace(keyspace, path) {
//   const chunks = [];

//   const entities = await loadEntities(keyspace);
//   const attributes = await loadAttributes(keyspace);
//   const relationships = await loadRelationships(keyspace);
//   const roles = await loadRoles(keyspace);

//   chunks.push(DEFINE_CHUNK);
//   chunks.push(serialiseEntities(entities));
//   chunks.push(serialiseAttributes(attributes));
//   chunks.push(serialiseRelationships(relationships));
//   chunks.push(serialiseRoles(roles));


//   writeToFile(chunks.join(''), path);
// }


// export default { exportKeyspace };
