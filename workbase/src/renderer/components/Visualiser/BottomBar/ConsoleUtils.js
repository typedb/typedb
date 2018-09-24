

async function conceptToString(concept) {
  let output = '';

  let sup;
  if (concept.isSchemaConcept()) sup = await concept.sup();

  switch (concept) {
    case concept.isAttribute():
      output += `val ${await concept.value()}`;
      break;
    case concept.isSchemaConcept():
      output += `label ${await concept.label()}`;

      if (sup) {
        output += ` sub ${await sup.label()}`;
      }
      break;
    default:
      output += `id ${concept.id}`;
      break;
  }

  if (concept.isRelationship()) {
    const rolePlayerList = [];

    const roleplayers = Array.from(((await concept.rolePlayersMap()).entries()));

    // Build array of promises
    const promises = Array.from(roleplayers, async ([role, setOfThings]) => {
      const roleLabel = await role.label();
      await Promise.all(Array.from(setOfThings.values()).map(async (thing) => {
        rolePlayerList.push(`${roleLabel}: id ${thing.id}`);
      }));
    });

    Promise.all(promises).then((() => {
      const relationString = rolePlayerList.join(', ');
      output += ` (${relationString})`;
    }));
  }

  if (concept.isThing()) {
    // const type = await (await concept.type()).label();
    output += ` isa ${await (await concept.type()).label()}`;
  }

  if (concept.isRule()) {
    output += ` when { await ${concept.getWhen()} }`;
    output += ` then { await ${concept.getThen()} }`;
  }
  //
  //   //////////
  //
  //   // Display any requested resources
  //   if (concept.isThing() && attributeTypes.length > 0) {
  //     concept.asThing().attributes(attributeTypes).forEach(resource -> {
  //       String resourceType = colorType(resource.type());
  //       String value = StringUtil.valueToString(resource.value());
  //       output.append(colorKeyword(" has ")).append(resourceType).append(" ").append(value);
  //     });
  //   }
  // ////////

  return output;
}

export default {
  conceptToString,
};
