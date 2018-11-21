import { dataType } from 'grakn';

function SchemaHandler(tx) {
  this.tx = tx;
}

function toGraknDatatype(dataTypeParam) {
  switch (dataTypeParam) {
    case 'string': return dataType.STRING;
    case 'date': return dataType.DATE;
    case 'boolean': return dataType.BOOLEAN;
    case 'long': return dataType.LONG;
    case 'double': return dataType.DOUBLE;
    default: throw new Error(`Datatype not recognised. Received [${dataTypeParam}]`);
  }
}

SchemaHandler.prototype.defineEntityType = async function define({ label, superType }) {
  const type = await this.tx.putEntityType(label);
  const directSuper = await this.tx.getSchemaConcept(superType);
  await type.sup(directSuper);
};

SchemaHandler.prototype.defineRole = async function define({ label, superType }) {
  const type = await this.tx.putRole(label);
  const directSuper = await this.tx.getSchemaConcept(superType);
  await type.sup(directSuper);
  return type;
};

SchemaHandler.prototype.defineRelationshipType = async function define({ label, superType }) {
  const type = await this.tx.putRelationshipType(label);
  const directSuper = await this.tx.getSchemaConcept(superType);
  await type.sup(directSuper);
  return type;
};

SchemaHandler.prototype.defineAttributeType = async function define({ label, superType, dataType, inheritDatatype }) {
  const type = await this.tx.putAttributeType(label, toGraknDatatype(dataType));
  if (!inheritDatatype) {
    const directSuper = await this.tx.getSchemaConcept(superType);
    await type.sup(directSuper);
  }
};

SchemaHandler.prototype.deleteType = async function deleteType({ label }) {
  const type = await this.tx.getSchemaConcept(label);
  await type.delete();
  return type.id;
};

SchemaHandler.prototype.addAttribute = async function addAttribute({ label, attributeLabel }) {
  const type = await this.tx.getSchemaConcept(label);
  const attribute = await this.tx.getSchemaConcept(attributeLabel);
  return type.has(attribute);
};

SchemaHandler.prototype.deleteAttribute = async function deleteAttribute({ label, attributeLabel }) {
  const type = await this.tx.getSchemaConcept(label);
  const attribute = await this.tx.getSchemaConcept(attributeLabel);
  return type.unhas(attribute);
};

SchemaHandler.prototype.addPlaysRole = async function addPlaysRole({ label, roleLabel }) {
  const type = await this.tx.getSchemaConcept(label);
  const role = await this.tx.getSchemaConcept(roleLabel);
  return type.plays(role);
};

SchemaHandler.prototype.deletePlaysRole = async function deletePlaysRole({ label, roleLabel }) {
  const type = await this.tx.getSchemaConcept(label);
  const role = await this.tx.getSchemaConcept(roleLabel);
  return type.unplay(role);
};

SchemaHandler.prototype.addRelatesRole = async function addRelatesRole({ label, roleLabel }) {
  const relationshipType = await this.tx.getSchemaConcept(label);
  const role = await this.tx.getSchemaConcept(roleLabel);
  return relationshipType.relates(role);
};

SchemaHandler.prototype.deleteRelatesRole = async function deleteRelatesRole({ label, roleLabel }) {
  const relationshipType = await this.tx.getSchemaConcept(label);
  const role = await this.tx.getSchemaConcept(roleLabel);
  await relationshipType.unrelate(role);
  return role.delete();
};

export default SchemaHandler;
