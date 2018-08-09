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
};

SchemaHandler.prototype.defineRelationshipType = async function define({ label, superType }) {
  const type = await this.tx.putRelationshipType(label);
  const directSuper = await this.tx.getSchemaConcept(superType);
  await type.sup(directSuper);
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


SchemaHandler.prototype.addAttribute = async function addAttribute({ label, typeLabel }) {
  const type = await this.tx.getSchemaConcept(label);
  const attribute = await this.tx.getSchemaConcept(typeLabel);
  return type.attribute(attribute);
};

SchemaHandler.prototype.deleteAttribute = async function deleteAttribute({ label, attributeName }) {
  const type = await this.tx.getSchemaConcept(label);
  const attribute = await this.tx.getSchemaConcept(attributeName);
  return type.removeAttribute(attribute);
};

SchemaHandler.prototype.addPlaysRole = async function addPlaysRole({ label, typeLabel }) {
  const type = await this.tx.getSchemaConcept(label);
  const role = await this.tx.getSchemaConcept(typeLabel);
  return type.plays(role);
};

SchemaHandler.prototype.deletePlaysRole = async function deletePlaysRole({ label, roleName }) {
  const type = await this.tx.getSchemaConcept(label);
  const role = await this.tx.getSchemaConcept(roleName);
  return type.deletePlays(role);
};

SchemaHandler.prototype.addRelatesRole = async function addRelatesRole({ label, typeLabel }) {
  const relationshipType = await this.tx.getSchemaConcept(label);
  const role = await this.tx.getSchemaConcept(typeLabel);
  return relationshipType.relates(role);
};

SchemaHandler.prototype.deleteRelatesRole = async function deleteRelatesRole({ label, roleName }) {
  const relationshipType = await this.tx.getSchemaConcept(label);
  const role = await this.tx.getSchemaConcept(roleName);
  return relationshipType.deleteRelates(role);
};

export default SchemaHandler;
