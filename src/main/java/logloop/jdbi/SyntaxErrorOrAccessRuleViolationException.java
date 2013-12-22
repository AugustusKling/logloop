package logloop.jdbi;

import java.sql.SQLException;

public class SyntaxErrorOrAccessRuleViolationException extends TypedSQLEception {
	private static final long serialVersionUID = 5367669976650533409L;

	public SyntaxErrorOrAccessRuleViolationException(SQLException e) {
		super(e);
	}
}
