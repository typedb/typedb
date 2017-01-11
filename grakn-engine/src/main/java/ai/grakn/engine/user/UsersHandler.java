package ai.grakn.engine.user;

import mjson.Json;

import java.util.HashMap;
import java.util.Map;

public class UsersHandler {
	public static final String USER_ENTITY = "user";
	public static final String USER_NAME = "user-name";
	public static final String USER_PASSWORD = "user-password";
	public static final String USER_SALT = "user-password-salt";
	public static final String USER_FIRST_NAME = "user-first-name";
	public static final String USER_LAST_NAME = "user-last-name";
	public static final String USER_EMAIL = "user-email";
	public static final String USER_IS_ADMIN = "user-is-admin";

	private static UsersHandler instance = null;
	private final Map<String, Json> usersMap = new HashMap<>();

	public synchronized static UsersHandler getInstance() {
		if (instance == null)
			instance = new SystemKeyspaceUsers(); // new UsersHandler();
		return instance;
	}
 
	protected UsersHandler() {		
	}

	public boolean addUser(Json user) {
		if (usersMap.containsKey(user.at(USER_NAME)))
			return false;
		usersMap.put(user.at(USER_NAME).asString(), user);
		return true;
	}

	public boolean updateUser(Json user) {
		if (usersMap.containsKey(user.at(USER_NAME)))
			return false;
		usersMap.put(user.at(USER_NAME).asString(), user);
		return true;
	}
	
	public boolean userExists(String username) {
		return usersMap.containsKey(username);
	}

	public boolean validateUser(String username, String hashedPassword) {
		if (userExists(username)) {
			return getUser(username).is(USER_PASSWORD, hashedPassword);
		}
		return false;
	}

	public Json getUser(String username) {
		return usersMap.get(username);
	}
	
	public boolean removeUser(String username) {
		return usersMap.remove(username) != null;
	}
	
	public Json allUsers(int offset, int limit) {
		return Json.make(usersMap.values()); 
	}
}