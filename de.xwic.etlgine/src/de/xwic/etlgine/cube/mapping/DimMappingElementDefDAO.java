/**
 * 
 */
package de.xwic.etlgine.cube.mapping;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple, direct JDBC usage DAO.
 * 
 * The DAO should be initialized for a series of operations and dropped after that.
 * 
 * @author lippisch
 */
public class DimMappingElementDefDAO {

	private Connection connection;
	private PreparedStatement psInsert;
	private PreparedStatement psUpdate;
	private PreparedStatement psDeleteByDimMapKey;
	private int orderIndex = 0;
	
	/**
	 * @param connection
	 * @throws SQLException 
	 */
	public DimMappingElementDefDAO(Connection connection) throws SQLException {
		super();
		this.connection = connection;
		
		psInsert = connection.prepareStatement("INSERT INTO [XCUBE_DIMMAP_ELEMENTS] (DimMapKey, Expression, isRegExp, IgnoreCase, ElementPath, SkipRecord, order_index) VALUES (?, ?, ?, ?, ?, ?, ?)");
		psUpdate = connection.prepareStatement("UPDATE [XCUBE_DIMMAP_ELEMENTS] SET Expression=?, isRegExp=?, IgnoreCase=?, ElementPath=?, SkipRecord=? WHERE ID = ?");
		psDeleteByDimMapKey = connection.prepareStatement("DELETE FROM [XCUBE_DIMMAP_ELEMENTS] WHERE DimMapKey = ?");
		
	}

	/**
	 * Returns the list of DimMappingDefs.
	 * @return
	 * @throws SQLException
	 */
	public List<DimMappingElementDef> listMappings() throws SQLException {
		
		List<DimMappingElementDef> list = new ArrayList<DimMappingElementDef>(); 
		
		Statement stmt = connection.createStatement();
		String sql = "SELECT [ID], DimMapKey, Expression, isRegExp, IgnoreCase, ElementPath, SkipRecord FROM XCUBE_DIMMAP_ELEMENTS ORDER BY DimMapKey, order_index ASC";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			DimMappingElementDef dmElm = new DimMappingElementDef();
			dmElm.setId(rs.getInt("ID"));
			dmElm.setDimMapKey(rs.getString("DimMapKey"));
			dmElm.setExpression(rs.getString("Expression"));
			dmElm.setRegExp(rs.getBoolean("isRegExp"));
			dmElm.setIgnoreCase(rs.getBoolean("IgnoreCase"));
			dmElm.setElementPath(rs.getString("ElementPath"));
			dmElm.setSkipRecord(rs.getBoolean("SkipRecord"));
			list.add(dmElm);
		}
		rs.close();
		stmt.close();
		
		return list;
	}

	/**
	 * Returns the list of DimMappingDefs.
	 * @return
	 * @throws SQLException
	 */
	public List<DimMappingElementDef> listMappings(String dimMapKey) throws SQLException {
		
		List<DimMappingElementDef> list = new ArrayList<DimMappingElementDef>(); 
		
		String sql = "SELECT [ID], DimMapKey, Expression, isRegExp, IgnoreCase, ElementPath, SkipRecord FROM XCUBE_DIMMAP_ELEMENTS WHERE DimMapKey = ? ORDER BY order_index ASC";
		PreparedStatement stmt = connection.prepareStatement(sql);
		stmt.setString(1, dimMapKey);
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			DimMappingElementDef dmElm = new DimMappingElementDef();
			dmElm.setId(rs.getInt("ID"));
			dmElm.setDimMapKey(rs.getString("DimMapKey"));
			dmElm.setExpression(rs.getString("Expression"));
			dmElm.setRegExp(rs.getBoolean("isRegExp"));
			dmElm.setIgnoreCase(rs.getBoolean("IgnoreCase"));
			dmElm.setElementPath(rs.getString("ElementPath"));
			dmElm.setSkipRecord(rs.getBoolean("SkipRecord"));
			list.add(dmElm);
		}
		rs.close();
		stmt.close();
		
		return list;
	}

	
	/**
	 * @param dimMapElm
	 * @throws SQLException 
	 */
	public void update(DimMappingElementDef dimMapElm) throws SQLException {
		
		//Expression=?, isRegExp=?, IgnoreCase=?, ElementPath=?, SkipRecord=? WHERE ID = ?
		int idx = 1;
		psUpdate.clearParameters();
		psUpdate.setString(idx++, dimMapElm.getExpression());
		psUpdate.setBoolean(idx++, dimMapElm.isRegExp());
		psUpdate.setBoolean(idx++, dimMapElm.isIgnoreCase());
		psUpdate.setString(idx++, dimMapElm.getElementPath());
		psUpdate.setBoolean(idx++, dimMapElm.isSkipRecord());
		psUpdate.setInt(idx++, dimMapElm.getId());
		
		int count = psUpdate.executeUpdate(); 
		if (count != 1) {
			throw new SQLException("Error updating DimMappingDef " + dimMapElm.getId() + ": Updated " + count + " but expected 1");
		}
		
	}

	/**
	 * @param dimMapElm
	 * @throws SQLException 
	 */
	public void insert(DimMappingElementDef dimMapElm) throws SQLException {
		insert(dimMapElm, orderIndex++);
	}
	
	/**
	 * Insert at the specified order index position.
	 * @param dimMapElm
	 * @param order_index
	 * @throws SQLException
	 */
	public void insert(DimMappingElementDef dimMapElm, int order_index) throws SQLException {

		//DimMapKey, Expression, isRegExp, IgnoreCase, ElementPath, SkipRecord
		int idx = 1;
		psInsert.clearParameters();
		psInsert.setString(idx++, dimMapElm.getDimMapKey());
		psInsert.setString(idx++, dimMapElm.getExpression());
		psInsert.setBoolean(idx++, dimMapElm.isRegExp());
		psInsert.setBoolean(idx++, dimMapElm.isIgnoreCase());
		psInsert.setString(idx++, dimMapElm.getElementPath());
		psInsert.setBoolean(idx++, dimMapElm.isSkipRecord());
		psInsert.setInt(idx++, order_index);
		int count = psInsert.executeUpdate(); 
		if (count != 1) {
			throw new SQLException("Error inserting DimMappingElementDef " + dimMapElm.getId() + ": Updated " + count + " but expected 1");
		}
		ResultSet rs = psInsert.getGeneratedKeys();
		if (rs.next()) {
			dimMapElm.setId(rs.getInt(1));
		}
		
	}

	/**
	 * @param key
	 * @throws SQLException 
	 */
	public int deleteByDimMapKey(String key) throws SQLException {

		psDeleteByDimMapKey.clearParameters();
		psDeleteByDimMapKey.setString(1, key);
		return psDeleteByDimMapKey.executeUpdate();
		
	}

	/**
	 * @return the orderIndex
	 */
	public int getOrderIndex() {
		return orderIndex;
	}
	
	/**
	 * @param orderIndex the orderIndex to set
	 */
	public void setOrderIndex(int orderIndex) {
		this.orderIndex = orderIndex;
	}
}
