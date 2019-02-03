package db;

public class DbException extends RuntimeException{
	
	private static final long serialVersionUID = 6552665333347001289L;
	
	public DbException(String msg) {
		super(msg);
	}

}
