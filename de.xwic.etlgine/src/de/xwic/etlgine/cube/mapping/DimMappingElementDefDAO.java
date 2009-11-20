/**
 * 
 */
package de.xwic.etlgine.cube.mapping;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.jdbc.JDBCUtil;

/**
 * Simple, direct JDBC usage DAO.
 * 
 * The DAO should be initialized for a series of operations and dropped after that.
 * 
 * @author lippisch
 */
public class DimMappingElementDefDAO {

	public final static String TABLE_NAME = "XCUBE_DIMMAP_ELEMENTS";
	
	private Connection connection;
	private PreparedStatement psInsert;
	private PreparedStatement psUpdate;
	private PreparedStatement psDeleteByDimMapKey;
	private PreparedStatement psDeleteById;
	
	private int orderIndex = 0;
	
	protected final Log log = LogFactory.getLog(getClass());
	
	/**
	 * @param connection
	 * @throws SQLException 
	 */
	public DimMappingElementDefDAO(Connection connection) throws SQLException {
		super();
		this.connection = connection;
		
		if (!JDBCUtil.columnExists(connection, TABLE_NAME, "ValidFrom")) {
			log.warn("Column 'ValidFrom', 'ValidTo' not found, trying to create column.");
			Statement stmt = connection.createStatement();
			stmt.execute("ALTER TABLE [" + TABLE_NAME + "] ADD [ValidFrom] DateTime, [ValidTo] DateTime");
			SQLWarning sw = stmt.getWarnings();
			if (sw != null) {
				log.warn("SQL Result: " + sw);
			}
			
		}
		
		psInsert = connection.prepareStatement("INSERT INTO [XCUBE_DIMMAP_ELEMENTS] (DimMapKey, Expression, isRegExp, IgnoreCase, ElementPath, SkipRecord, order_index, ValidFrom, ValidTo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
		psUpdate = connection.prepareStatement("UPDATE [XCUBE_DIMMAP_ELEMENTS] SET Expression=?, isRegExp=?, IgnoreCase=?, ElementPath=?, SkipRecord=?, ValidFrom=?, ValidTo=? WHERE ID = ?");
		psDeleteByDimMapKey = connection.prepareStatement("DELETE FROM [XCUBE_DIMMAP_ELEMENTS] WHERE DimMapKey = ?");
		psDeleteById = connection.prepareStatement("DELETE FROM [XCUBE_DIMMAP_ELEMENTS] WHERE ID = ?");
	}

	/**
	 * Returns the list of DimMappingDefs.
	 * @return
	 * @throws SQLException
	 */
	public List<DimMappingElementDef> listMappings() throws SQLException {
		
		List<DimMappingElementDef> list = new ArrayList<DimMappingElementDef>(); 
		
		Statement stmt = connection.createStatement();
		String sql = "SELECT [ID], DimMapKey, Expression, isRegExp, IgnoreCase, ElementPath, SkipRecord, ValidFrom, ValidTo FROM XCUBE_DIMMAP_ELEMENTS ORDER BY DimMapKey, order_index ASC";
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
			dmElm.setValidFrom(rs.getDate("ValidFrom"));
			dmElm.setValidTo(rs.getDate("ValidTo"));
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
		
		String sql = "SELECT [ID], DimMapKey, Expression, isRegExp, IgnoreCase, ElementPath, SkipRecord, ValidFrom, ValidTo FROM XCUBE_DIMMAP_ELEMENTS WHERE DimMapKey = ? ORDER BY order_index ASC";
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
			dmElm.setValidFrom(rs.getDate("ValidFrom"));
			dmElm.setValidTo(rs.getDate("ValidTo"));
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
		if (dimMapElm.getValidFrom() != null) {
			psUpdate.setDate(idx++, new java.sql.Date(dimMapElm.getValidFrom().getTime()));
		} else {
			psUpdate.setNull(idx++, Types.DATE);
		}
		if (dimMapElm.getValidTo() != null) {
			psUpdate.setDate(idx++, new java.sql.Date(dimMapElm.getValidTo().getTime()));
		} else {
			psUpdate.setNull(idx++, Types.DATE);
		}
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
		if (dimMapElm.getValidFrom() != null) {
			psInsert.setDate(idx++, new java.sql.Date(dimMapElm.getValidFrom().getTime()));
		} else {
			psInsert.setNull(idx++, Types.DATE);
		}
		if (dimMapElm.getValidTo() != null) {
			psInsert.setDate(idx++, new java.sql.Date(dimMapElm.getValidTo().getTime()));
		} else {
			psInsert.setNull(idx++, Types.DATE);
		}
		
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
	 * @param id
	 * @throws SQLException 
	 */
	public int deleteById(int id) throws SQLException {
		psDeleteById.clearParameters();
		psDeleteById.setInt(1, id);
		return psDeleteById.executeUpdate();
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
