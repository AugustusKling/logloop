package logloop;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;

import logloop.jdbi.TypedSQLEception;
import logloop.jdbi.SyntaxErrorOrAccessRuleViolation.UndefinedTableException;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

/**
 * Log event. This is stored in a daily child relation of the logloop.events
 * relation. The child relations are automatically created upon first use.
 */
public class Event {
	public final Timestamp timestamp;
	public final String message;
	public final Map<String, String> fields;
	/**
	 * Unique event identifier.
	 */
	public final UUID uuid;

	public Event(UUID uuid, Timestamp timestamp, String message,
			Map<String, String> fields) {
		this.uuid = uuid;
		this.timestamp = timestamp;
		this.message = message;
		this.fields = fields;
	}

	/**
	 * Persists the event.
	 */
	public void store(DBI dbi) {
		try (Handle h = dbi.open()) {
			try {
				String relation = getRelation();
				Update query = h
						.createStatement("insert into "
								+ relation
								+ " (uuid, timestamp, message, fields) values (#uuid, #timestamp, #message, #fields);");
				query.bind("uuid", uuid);
				query.bind("timestamp", timestamp);
				query.bind("message", message);
				query.bind("fields", fields);
				query.execute();
			} catch (UnableToExecuteStatementException e) {
				if (TypedSQLEception.type(e) instanceof UndefinedTableException) {
					// Attempt to create the daily child relation.
					createRelation(dbi);
					// Retry to persist.
					store(dbi);
				} else {
					throw e;
				}
			}
		}
	}

	/**
	 * Creates a child relation of logloop.events to group the events by day.
	 */
	private void createRelation(DBI dbi) {
		String relation = getRelation();

		Calendar time = Calendar.getInstance();
		time.setTimeInMillis(timestamp.getTime());

		Calendar dayBegin = (Calendar) time.clone();
		dayBegin.set(Calendar.HOUR_OF_DAY, 0);
		dayBegin.set(Calendar.MINUTE, 0);
		dayBegin.set(Calendar.SECOND, 0);
		dayBegin.set(Calendar.MILLISECOND, 0);

		Calendar dayEnd = (Calendar) dayBegin.clone();
		dayEnd.add(Calendar.DATE, 1);

		try (Handle h = dbi.open()) {
			h.createStatement(
					"CREATE TABLE " + relation + "(" + "  CONSTRAINT pk_"
							+ getRelationName() + "_uuid PRIMARY KEY (uuid),"
							+ "  CONSTRAINT chk_" + getRelationName()
							+ "_timestamp CHECK (\"timestamp\" >= '"
							+ toYMD(dayBegin) + "'::date AND \"timestamp\" < '"
							+ toYMD(dayEnd) + "'::date)" + ")"
							+ " INHERITS (logloop.events)").execute();
			h.createStatement(
					"CREATE INDEX ix_" + getRelationName() + "_timestamp ON "
							+ relation + " USING btree (\"timestamp\");")
					.execute();
		}
	}

	private String toYMD(Calendar time) {
		return time.get(Calendar.YEAR) + "-" + (time.get(Calendar.MONTH) + 1)
				+ "-" + time.get(Calendar.DATE);
	}

	/**
	 * @return Name of daily child relation with schema.
	 */
	private String getRelation() {
		return "logloop." + getRelationName();
	}

	/**
	 * @return Name of daily child relation without schema.
	 */
	private String getRelationName() {
		Calendar time = Calendar.getInstance();
		time.setTimeInMillis(timestamp.getTime());
		return "events_y" + time.get(Calendar.YEAR) + "m"
				+ (time.get(Calendar.MONTH) + 1) + "d"
				+ time.get(Calendar.DATE);
	}

}
