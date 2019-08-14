define

example-id sub attribute,
    datatype long;

person sub entity,
    has example-id,
    plays patient,
    plays symptomatic-patient;

disease sub entity,
    plays cause,
    plays diagnosed-disease;

diagnosis sub relation,
    relates patient,
    relates diagnosed-disease;

candidate-diagnosis sub relation,
    relates candidate-patient,
    relates candidate-diagnosed-disease;

where-no-diagnosis-add-candidate-diagnosis sub rule,
    when {
        $p isa person;
        $d isa disease;
        not{ (patient: $p, diagnosed-disease: $d) isa diagnosis; };
    }, then {
        (candidate-patient: $p, candidate-diagnosed-disease: $d) isa candidate-diagnosis;
    };


meningitis sub disease;
flu sub disease;

causality sub relation,
    relates cause,
    relates effect;

symptom sub entity,
    plays presented-symptom,
    plays effect;

light-sensitivity sub symptom;
fever sub symptom;

symptom-presentation sub relation,
    relates presented-symptom,
    relates symptomatic-patient;

# Add elements to the Knowledge Graph that are common to all examples
insert
    $men isa meningitis;
    $flu isa flu;
    $ls isa light-sensitivity;
    $fever isa fever;
    $c(cause: $men, effect: $ls) isa causality;
    $c1(cause: $men, effect: $fever) isa causality;
    $c2(cause: $flu, effect: $ls) isa causality;
    $c3(cause: $flu, effect: $fever) isa causality;