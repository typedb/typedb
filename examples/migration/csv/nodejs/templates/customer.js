function customerTemplate(data) {
  const { id, first_name, last_name, phone_number, city, age } = data;

  return (
    'insert $customer isa customer has firstname "' +
    first_name +
    '", has lastname "' +
    last_name +
    '", has phonenumber "' +
    phone_number +
    '", has city "' +
    city +
    '", has age ' +
    age +
    ', has identifier "' +
    id +
    '";'
  );
}

module.exports = customerTemplate;
