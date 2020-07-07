package grakn.core.test.behaviour.connection.session;

import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlQuery;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.core.test.behaviour.connection.ConnectionSteps.sessions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Manages a set of Grakn sessions, ensuring there is at most one concurrent session in any keyspace,
 * and one concurrent transaction in any session.
 */
public class SessionManager implements AutoCloseable {
	private String primaryKeyspaceName;
	private final Map<Session, Transaction> transactions;
	private List<ConceptMap> answers;
	private List<Numeric> numericAnswers;
	private List<AnswerGroup<ConceptMap>> answerGroups;
	private List<AnswerGroup<Numeric>> numericAnswerGroups;

	public SessionManager() {
		transactions = new HashMap<>();
		primaryKeyspaceName = "test";
	}

	public void createSessions(final Collection<Session> sessions) {
		sessions.forEach(s -> transactions.put(s, null));
	}

	public void execute(final Session session, final Consumer<Transaction> actions) {
		try (Transaction tx = getOpenTransaction(session)) {
			actions.accept(tx);
			tx.commit();
		}
	}

	public <T> T execute(final Session session, final Function<Transaction, T> queries) {
		try (Transaction tx = getOpenTransaction(session)) {
			final T result = queries.apply(tx);
			tx.commit();
			return result;
		}
	}

	public <TQuery extends GraqlQuery> void executeGraqlQuery(
			final String queryStatements,
			final Function<GraqlQuery, TQuery> queryTypeFn,
			final boolean commit) {

		for (final Session session : transactions.keySet()) {
			final Transaction tx = getOpenTransaction(session);
			try {
				final TQuery graqlQuery = queryTypeFn.apply(Graql.parse(String.join("\n", queryStatements)));
				tx.execute(graqlQuery);
				if (commit) {
					tx.commit();
				}
			} catch (RuntimeException e) {
				tx.close();
				throw e;
			}
		}
	}

	public void storeAnswersOfGraqlInsert(final String queryStatements) {
		final Transaction tx = getOpenTransactionForPrimarySession();
		final GraqlInsert graqlQuery = Graql.parse(String.join("\n", queryStatements)).asInsert();
		answers = tx.execute(graqlQuery, true, true);
		tx.commit();
	}

	public void storeAnswersOfGraqlQuery(String graqlQueryStatements) {
		GraqlQuery graqlQuery = Graql.parse(String.join("\n", graqlQueryStatements));
		erasePreviousAnswers();
		final Transaction tx = getOpenTransactionForPrimarySession();
		assertFalse("Insert is not supported; use storeAnswersOfGraqlInsert instead", graqlQuery instanceof GraqlInsert);
		if (graqlQuery instanceof GraqlGet) {
			answers = tx.execute(graqlQuery.asGet(), true, true); // always use inference and have explanations
		} else if (graqlQuery instanceof GraqlGet.Aggregate) {
			numericAnswers = tx.execute(graqlQuery.asGetAggregate());
		} else if (graqlQuery instanceof GraqlGet.Group) {
			answerGroups = tx.execute(graqlQuery.asGetGroup());
		} else if (graqlQuery instanceof GraqlGet.Group.Aggregate) {
			numericAnswerGroups = tx.execute(graqlQuery.asGetGroupAggregate());
		} else {
			fail("Only match-get, aggregate, group and group aggregate supported for now");
		}
	}

	public Set<Session> getSessions() {
		return transactions.keySet();
	}

	public Session getSession(final String keyspaceName) {
		final List<Session> matchingSessions = sessions.stream()
				.filter(s -> s.keyspace().name().equalsIgnoreCase(keyspaceName))
				.collect(Collectors.toList());
		assertEquals(1, matchingSessions.size());
		return matchingSessions.get(0);
	}

	public Session getPrimarySession() {
		final List<Session> testSessions = sessions.stream()
				.filter(s -> s.keyspace().name().equalsIgnoreCase(primaryKeyspaceName))
				.collect(Collectors.toList());
		assertEquals(
				String.format("There must be exactly one test session. If there are multiple sessions, " +
						"there must be one in a keyspace named [%s]", primaryKeyspaceName),
				1,
				testSessions.size());
		return testSessions.get(0);
	}

	public Transaction getOpenTransactionForPrimarySession() {
		return getOpenTransaction(getPrimarySession());
	}

	public Transaction getOpenTransaction(final Session session) {
		Transaction tx = transactions.get(session);
		if (tx != null && tx.isOpen()) {
			return tx;
		}
		return newTransaction(session);
	}

	public List<ConceptMap> getAnswers() {
		return answers;
	}

	public List<AnswerGroup<ConceptMap>> getAnswerGroups() {
		return answerGroups;
	}

	public List<Numeric> getNumericAnswers() {
		return numericAnswers;
	}

	public List<AnswerGroup<Numeric>> getNumericAnswerGroups() {
		return numericAnswerGroups;
	}

	public void setPrimaryKeyspaceName(final String value) {
		primaryKeyspaceName = value;
	}

	public String getPrimaryKeyspaceName() {
		return primaryKeyspaceName;
	}

	/**
	 * Closes all transactions and their corresponding sessions.
	 */
	@Override
	public void close() {
		transactions.values().forEach(Transaction::close);
		transactions.keySet().forEach(Session::close);
	}

	private Transaction newTransaction(final Session session) {
		final Transaction tx = session.transaction(Transaction.Type.WRITE);
		transactions.put(session, tx);
		return tx;
	}

	private void erasePreviousAnswers() {
		answers = null;
		numericAnswers = null;
		answerGroups = null;
		numericAnswerGroups = null;
	}
}
