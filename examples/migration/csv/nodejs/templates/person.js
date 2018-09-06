const personTemplate = data => {
  const { phone_number, id } = data;

  return (
    'insert $person isa person has phonenumber "' +
    phone_number +
    '", has identifier "' +
    id +
    '";'
  );
};

module.exports = personTemplate;
