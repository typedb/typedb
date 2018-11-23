

import SchemaHandler from '@/components/SchemaDesign/SchemaHandler';
import ServerSettings from '@/components/ServerSettings';

import Grakn from 'grakn';

jest.mock('@/components/shared/PersistentStorage', () => ({
//   get: jest.fn(),
}));

jest.mock('@/components/ServerSettings', () => ({
  getServerHost: () => '127.0.0.1',
  getServerUri: () => '127.0.0.1:48555',
}));

jest.setTimeout(30000);

const grakn = new Grakn(ServerSettings.getServerUri(), null);
let graknSession;

beforeEach(() => {
  graknSession = grakn.session('testkeyspace');
});


describe('Actions', () => {
  test('define entity type', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.defineEntityType({ label: 'person' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    expect(await graknTx.getSchemaConcept('person')).toBeDefined();
  });

  test('define attribute type', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.defineAttributeType({ label: 'name', dataType: 'string' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    expect(await graknTx.getSchemaConcept('name')).toBeDefined();
  });

  test('define relationship type & role type', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.defineRelationshipType({ label: 'parentship' });
    await schemaHandler.defineRole({ label: 'parent' });
    await schemaHandler.addRelatesRole({ label: 'parentship', roleLabel: 'parent' });

    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    expect(await graknTx.getSchemaConcept('parent')).toBeDefined();

    const parentshipRoles = await (await (await graknTx.getSchemaConcept('parentship')).roles()).collect();
    expect(parentshipRoles).toHaveLength(1);
  });

  test('delete type', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    let schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.defineEntityType({ label: 'delete-type' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    expect(await graknTx.getSchemaConcept('delete-type')).toBeDefined();

    schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.deleteType({ label: 'delete-type' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    expect(await graknTx.getSchemaConcept('delete-type')).toBe(null);
  });

  test('add attribute', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.addAttribute({ label: 'person', attributeLabel: 'name' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const personAttributes = await (await (await graknTx.getSchemaConcept('person')).attributes()).collect();
    expect(personAttributes).toHaveLength(1);
  });

  test('delete attribute', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    let schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.addAttribute({ label: 'person', attributeLabel: 'name' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    let personAttributes = await (await (await graknTx.getSchemaConcept('person')).attributes()).collect();
    expect(personAttributes).toHaveLength(1);

    schemaHandler = new SchemaHandler(graknTx);
    await schemaHandler.deleteAttribute({ label: 'person', attributeLabel: 'name' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    personAttributes = await (await (await graknTx.getSchemaConcept('person')).attributes()).collect();
    expect(personAttributes).toHaveLength(0);
  });

  test('add plays role', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.addPlaysRole({ label: 'person', roleLabel: 'parent' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const personRoles = await (await (await graknTx.getSchemaConcept('person')).playing()).collect();
    expect(personRoles).toHaveLength(1);
  });

  test('delete plays role', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.deletePlaysRole({ label: 'person', roleLabel: 'parent' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const personRoles = await (await (await graknTx.getSchemaConcept('person')).playing()).collect();
    expect(personRoles).toHaveLength(0);
  });

  test('delete relates role', async () => {
    let graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    let schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.defineRelationshipType({ label: 'parentship' });
    await schemaHandler.defineRole({ label: 'parent' });
    await schemaHandler.addRelatesRole({ label: 'parentship', roleLabel: 'parent' });

    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);

    const personRoles = await (await (await graknTx.getSchemaConcept('parentship')).roles()).collect();
    expect(personRoles).toHaveLength(1);

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    schemaHandler = new SchemaHandler(graknTx);

    await schemaHandler.deleteRelatesRole({ label: 'parentship', roleLabel: 'parent' });
    await graknTx.commit();

    graknTx = await graknSession.transaction(Grakn.txType.WRITE);
    const parentshipRoles = await (await (await graknTx.getSchemaConcept('parentship')).roles()).collect();
    expect(parentshipRoles).toHaveLength(0);
  });
});

