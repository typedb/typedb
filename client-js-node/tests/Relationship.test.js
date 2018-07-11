const env = require('./support/GraknTestEnvironment');
let session;
let tx;

beforeAll(() => {
    session = env.session();
});

afterAll(async () => {
    await env.tearDown();
});

beforeEach(async () => {
    tx = await session.transaction(env.txType().WRITE);
})

afterEach(() => {
    tx.close();
});

describe("Relationsihp methods", () => {

    test("allRolePlayers && rolePlayers with 2 roles with 1 player each", async () => {
        const relationshipType = await tx.putRelationshipType('parenthood');
        const relationship = await relationshipType.addRelationship();
        const parentRole = await tx.putRole('parent');
        const childRole = await tx.putRole('child');
        const personType = await tx.putEntityType('person');
        const parent = await personType.addEntity();
        const child = await personType.addEntity();
        await relationship.addRolePlayer(parentRole, parent);
        await relationship.addRolePlayer(childRole, child);
        const map = await relationship.allRolePlayers();
        expect(map.size).toBe(2);
        Array.from(map.keys()).forEach(key => { expect(key.isRole()).toBeTruthy(); });
        Array.from(map.values()).forEach(set => { expect(Array.from(set).length).toBe(1); });
        const rolePlayers = await relationship.rolePlayers();
        expect(rolePlayers.length).toBe(2);
    });

    test("allRolePlayers && rolePlayers with 1 role with 2 players", async () => {
        const relationshipType = await tx.putRelationshipType('parenthood');
        const relationship = await relationshipType.addRelationship();
        const parentRole = await tx.putRole('parent');
        const personType = await tx.putEntityType('person');
        const parent = await personType.addEntity();
        const anotherParent = await personType.addEntity();
        await relationship.addRolePlayer(parentRole, parent);
        await relationship.addRolePlayer(parentRole, anotherParent);
        const map = await relationship.allRolePlayers();
        expect(map.size).toBe(1);
        Array.from(map.keys()).forEach(key => { expect(key.isRole()).toBeTruthy(); });
        Array.from(map.values()).forEach(set => { expect(Array.from(set).length).toBe(2); });
        const rolePlayers = await relationship.rolePlayers();
        expect(rolePlayers.length).toBe(2);
    });

    test("allRolePlayers && rolePlayers with 2 roles with the same player", async () => {
        const relationshipType = await tx.putRelationshipType('parenthood');
        const relationship = await relationshipType.addRelationship();
        const parentRole = await tx.putRole('parent');
        const childRole = await tx.putRole('child');
        const personType = await tx.putEntityType('person');
        const parentOfHimself = await personType.addEntity();
        await relationship.addRolePlayer(parentRole, parentOfHimself);
        await relationship.addRolePlayer(childRole, parentOfHimself);
        const map = await relationship.allRolePlayers();
        expect(map.size).toBe(2);
        Array.from(map.keys()).forEach(key => { expect(key.isRole()).toBeTruthy(); });
        Array.from(map.values()).forEach(set => { expect(Array.from(set).length).toBe(1); });
        const rolePlayers = await relationship.rolePlayers();
        expect(rolePlayers.length).toBe(1);
    });

    test("addRolePlayer && removeRolePlayer && rolePlayers", async () => {
        const relationshipType = await tx.putRelationshipType('parenthood');
        const relationship = await relationshipType.addRelationship();
        const parentRole = await tx.putRole('parent');
        const personType = await tx.putEntityType('person');
        const person = await personType.addEntity();
        const emptyRolePlayers = await relationship.rolePlayers();
        expect(emptyRolePlayers.length).toBe(0);
        await relationship.addRolePlayer(parentRole, person);
        const rolePlayers = await relationship.rolePlayers();
        expect(rolePlayers.length).toBe(1);
        expect(rolePlayers[0].isThing()).toBeTruthy();
        expect(rolePlayers[0].id).toBe(person.id);
        await relationship.removeRolePlayer(parentRole, person);
        const rolePlayersRemoved = await relationship.rolePlayers();
        expect(rolePlayersRemoved.length).toBe(0);

    });

    test("rolePlayers(...Role)", async () => {
        const relationshipType = await tx.putRelationshipType('parenthood');
        const relationship = await relationshipType.addRelationship();
        const parentRole = await tx.putRole('parent');
        const childRole = await tx.putRole('child');
        const personType = await tx.putEntityType('person');
        const parent = await personType.addEntity();
        const child = await personType.addEntity();
        await relationship.addRolePlayer(parentRole, parent);
        await relationship.addRolePlayer(childRole, child);
        const rolePlayers = await relationship.rolePlayers();
        expect(rolePlayers.length).toBe(2);
        const filteredRolePlayers = await relationship.rolePlayers(childRole);
        expect(filteredRolePlayers.length).toBe(1);
        const player = filteredRolePlayers[0];
        expect(player.id).toBe(child.id);
        const doubleRolePlayers = await relationship.rolePlayers(childRole, parentRole);
        expect(doubleRolePlayers.length).toBe(2);
    });
});