package io.mindmaps.api;

import io.mindmaps.loader.Loader;

import java.util.UUID;

import static spark.Spark.post;

public class TransactionController {

    public TransactionController() {

        post("/transaction", (request, response) -> {
            UUID uuid = Loader.getInstance().addJob(request.body());
            if (uuid != null) {
                response.status(201);
                return uuid.toString();
            } else {
                response.status(405);
                return "Error";
            }
        });


    }


    //ASK FILIPE


//    @RequestMapping(method = RequestMethod.GET)
//    public ResponseEntity<Resources<Transaction>> listTransaction() {
//        Stream<Transaction> transactions = queueManager.getStates().keySet().stream().map(Transaction::new);
//        Link listLink = linkTo(methodOn(TransactionController.class).listTransaction()).withRel("list");
//        Link postLink = linkTo(methodOn(TransactionController.class).postTransaction(null)).withRel("create");
//        return new ResponseEntity<Resources<Transaction>>(new Resources<>(transactions::iterator, listLink, postLink), HttpStatus.OK);
//    }
//
//    @RequestMapping(value = "/{uuid}", method = RequestMethod.GET)
//    public ResponseEntity<Transaction> getTransaction(@PathVariable UUID uuid) {
//        return new ResponseEntity<>(new Transaction(uuid), HttpStatus.OK);
//    }
//
//    class Transaction extends ResourceSupport {
//
//        private final UUID uuid;
//
//        public UUID getUuid() {
//            return uuid;
//        }
//
//        public State getState() {
//            return TransactionController.this.queueManager.getState(uuid);
//        }
//
//        public List<String> getErrors() {
//            return TransactionController.this.queueManager.getErrors(uuid);
//        }
//
//        public Transaction(UUID uuid) {
//            // Add 'self' href
//            add(linkTo(methodOn(TransactionController.class).getTransaction(uuid)).withSelfRel());
//            this.uuid = uuid;
//        }
//    }
}
