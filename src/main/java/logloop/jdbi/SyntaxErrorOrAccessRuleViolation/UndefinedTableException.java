package logloop.jdbi.SyntaxErrorOrAccessRuleViolation;

import java.sql.SQLException;

import logloop.jdbi.SyntaxErrorOrAccessRuleViolationException;

public class UndefinedTableException extends SyntaxErrorOrAccessRuleViolationException {
	private static final long serialVersionUID = -3504242196965329992L;

	public UndefinedTableException(SQLException e) {
		super(e);
	}

}
