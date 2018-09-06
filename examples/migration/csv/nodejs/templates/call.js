function callTemplate(data) {
  const {
    id,
    caller_id,
    callee_as_person_id,
    callee_as_customer_id,
    started_at,
    duration
  } = data;

  // match caller
  let query =
    'match $caller isa customer, has identifier "' + caller_id + '"; ';

  // match callee
  if (callee_as_person_id) {
    query +=
      '$callee isa person, has identifier "' + callee_as_person_id + '"; ';
  } else if (callee_as_customer_id) {
    query +=
      '$callee isa customer, has identifier "' + callee_as_customer_id + '"; ';
  }

  // insert relationship
  query +=
    "insert $call(caller: $caller, callee: $callee) isa call; $call has startedat " +
    started_at +
    "; $call has duration " +
    duration +
    '; $call has identifier "' +
    id +
    '";';

  return query;
}

module.exports = callTemplate;
