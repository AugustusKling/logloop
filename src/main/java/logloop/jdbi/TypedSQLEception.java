package logloop.jdbi;

import java.sql.SQLException;

import logloop.jdbi.SyntaxErrorOrAccessRuleViolation.UndefinedTableException;

import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

/**
 * Spans hierarchy to replicate SQL errors.
 */
public class TypedSQLEception extends SQLException {
	private static final long serialVersionUID = 249973698060423057L;

	protected TypedSQLEception(Exception e) {
		super(e);
	}

	/**
	 * Parses SQL error code.
	 * 
	 * @param e
	 *            General database error that possibly contains a closer
	 *            description in the form of an error code.
	 * @return More specifically typed exception.
	 */
	public static TypedSQLEception type(UnableToExecuteStatementException e) {
		Throwable cause = e.getCause();
		if (cause instanceof SQLException) {
			SQLException sqlException = ((SQLException) cause);
			String code = sqlException.getSQLState();
			switch (code) {
			case "42P01":
				return new UndefinedTableException(sqlException);
			default: {
				String group = code.substring(0, 2);
				switch (group) {
				case "42":
					return new SyntaxErrorOrAccessRuleViolationException(
							sqlException);
				default:
					return new TypedSQLEception(e);
				}
			}
			}
		} else {
			return new TypedSQLEception(e);
		}
	}
}
