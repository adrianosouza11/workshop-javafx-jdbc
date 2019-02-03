package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao{
	private Connection connection = null;
	
	public DepartmentDaoJDBC(Connection connection) {
		this.connection = connection;
	}
	
	@Override
	public void insert(Department obj) {
		PreparedStatement st = null;
		
		try {
			st = connection.prepareStatement("INSERT INTO department (Name) VALUES (?) ", Statement.RETURN_GENERATED_KEYS);
			st.setString(1, obj.getName());
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected > 0) {
				System.out.println("DONE! Rows affected: " + rowsAffected);
				ResultSet rs = st.getGeneratedKeys();
				if(rs.next()) {
					obj.setId(rs.getInt(1));
					System.out.println(obj.getId());
				}
			}else {
				throw new SQLException("Error: No insert");
			}
		}catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		
	}

	@Override
	public void update(Department obj) {
		PreparedStatement st = null;
		
		try {
			st = connection.prepareStatement("UPDATE department SET Name = ? WHERE id = ? ");
			
			st.setString(1, obj.getName());
			st.setInt(2, obj.getId());
			
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected > 0) {
				System.out.println("DONE! Rows affected: " + rowsAffected);
			}else {
				throw new SQLException("Error: No Update");
			}
		}catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		try {
			st = connection.prepareStatement("DELETE FROM department WHERE id = ?");
			st.setInt(1, id);
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected > 0) {
				System.out.println("Done! Delete is done.");
			}else {
				throw new SQLException("Error: Delete no change.");
			}
			
		}catch(SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement st = null;
		ResultSet resultSet = null;
		try {
			st = connection.prepareStatement("SELECT * FROM department WHERE id = ?");
			st.setInt(1, id);
			resultSet = st.executeQuery();
			
			if(!resultSet.next()) {
				return null;		
			}
			
			return implementationDepartment(resultSet);
		}catch(SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeResultSet(resultSet);
			DB.closeStatement(st);
		}
		
	}

	@Override
	public List<Department> findAll() {
		 PreparedStatement st = null;
		 ResultSet resultSet = null;
		 try {
			 st = connection.prepareStatement("SELECT * FROM department");
			 resultSet = st.executeQuery();
			 
			 List<Department> list = new ArrayList<>();
			 
			 while(resultSet.next()) {
				 list.add(implementationDepartment(resultSet));
			 }
			 
			 return list;
		 }catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeResultSet(resultSet);
			DB.closeStatement(st);
		}
	}
	
	public Department implementationDepartment(ResultSet resultSet) throws SQLException {
		return new Department(resultSet.getInt("Id"), resultSet.getString("Name"));
	}

}
