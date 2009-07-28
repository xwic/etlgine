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
public class DimMappingDefDAO {

	private final static String TABLE_NAME = "XCUBE_DIMMAP";
	
	private Connection connection;
	private PreparedStatement psInsert;
	private PreparedStatement psUpdate;

	protected final Log log = LogFactory.getLog(getClass());
	
	/**
	 * @param connection
	 * @throws SQLException 
	 */
	public DimMappingDefDAO(Connection connection) throws SQLException {
		super();
		this.connection = connection;
		
		psInsert = connection.prepareStatement("INSERT INTO [" + TABLE_NAME + "] (DimMapKey, Description, DimensionKey, UnmappedPath, OnUnmapped, AutoCreate) VALUES (?, ?, ?, ?, ?, ?)");
		psUpdate = connection.prepareStatement("UPDATE [" + TABLE_NAME + "] SET Description=?, DimensionKey=?, UnmappedPath=?, OnUnmapped=?, AutoCreate=? WHERE DimMapKey = ?");
		
		// check for missing columns
		if (!JDBCUtil.columnExists(connection, TABLE_NAME, "AutoCreate")) {
			// create column
			log.warn("Column 'AutoCreate' not found, trying to create column.");
			Statement stmt = connection.createStatement();
			stmt.execute("ALTER TABLE [" + TABLE_NAME + "] ADD [AutoCreate] Bit NOT NULL Default 0");
			SQLWarning sw = stmt.getWarnings();
			if (sw != null) {
				log.warn("SQL Result: " + sw);
			}
		}
		
	}

	/**
	 * Returns the list of DimMappingDefs.
	 * @return
	 * @throws SQLException
	 */
	public List<DimMappingDef> listMappings() throws SQLException {
		
		List<DimMappingDef> list = new ArrayList<DimMappingDef>(); 
		
		Statement stmt = connection.createStatement();
		String sql = "SELECT [DimMapKey], [Description], [DimensionKey], [UnmappedPath], [OnUnmapped], [AutoCreate] FROM " + TABLE_NAME;
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			DimMappingDef dmd = new DimMappingDef();
			dmd.setKey(rs.getString("DimMapKey"));
			dmd.setDescription(rs.getString("Description"));
			dmd.setDimensionKey(rs.getString("DimensionKey"));
			dmd.setUnmappedPath(rs.getString("UnmappedPath"));
			dmd.setOnUnmapped(DimMappingDef.Action.valueOf(rs.getString("OnUnmapped")));
			dmd.setAutoCreateMapping(rs.getBoolean("AutoCreate"));
			list.add(dmd);
		}
		rs.close();
		stmt.close();
		
		return list;
	}

	/**
	 * Find the mapping with the specified key.
	 * @param dimMapKey
	 * @return
	 * @throws SQLException
	 */
	public DimMappingDef findMapping(String dimMapKey) throws SQLException {
		DimMappingDef dmd = null;
		String sql = "SELECT [DimMapKey], [Description], [DimensionKey], [UnmappedPath], [OnUnmapped], [AutoCreate] FROM " + TABLE_NAME + " WHERE [DimMapKey] = ?";
		PreparedStatement stmt = connection.prepareStatement(sql);
		stmt.setString(1, dimMapKey);
		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			dmd = new DimMappingDef();
			dmd.setKey(rs.getString("DimMapKey"));
			dmd.setDescription(rs.getString("Description"));
			dmd.setDimensionKey(rs.getString("DimensionKey"));
			dmd.setUnmappedPath(rs.getString("UnmappedPath"));
			dmd.setOnUnmapped(DimMappingDef.Action.valueOf(rs.getString("OnUnmapped")));
			dmd.setAutoCreateMapping(rs.getBoolean("AutoCreate"));
		}
		rs.close();
		stmt.close();
		return dmd;
	}
	
	/**
	 * @param dimMapping
	 * @throws SQLException 
	 */
	public void update(DimMappingDef dimMapping) throws SQLException {
		
		//Description=?, DimensionKey=?, UnmappedPath=?, OnUnmapped=? WHERE DimMapKey = ?
		int idx = 1;
		psUpdate.clearParameters();
		psUpdate.setString(idx++, dimMapping.getDescription());
		psUpdate.setString(idx++, dimMapping.getDimensionKey());
		psUpdate.setString(idx++, dimMapping.getUnmappedPath());
		psUpdate.setString(idx++, dimMapping.getOnUnmapped().name());
		psUpdate.setBoolean(idx++, dimMapping.isAutoCreateMapping());
		psUpdate.setString(idx++, dimMapping.getKey());
		int count = psUpdate.executeUpdate(); 
		if (count != 1) {
			throw new SQLException("Error updating DimMappingDef " + dimMapping.getKey() + ": Updated " + count + " but expected 1");
		}
		
	}

	/**
	 * @param dimMapping
	 * @throws SQLException 
	 */
	public void insert(DimMappingDef dimMapping) throws SQLException {

		int idx = 1;
		psInsert.clearParameters();
		psInsert.setString(idx++, dimMapping.getKey());
		psInsert.setString(idx++, dimMapping.getDescription());
		psInsert.setString(idx++, dimMapping.getDimensionKey());
		psInsert.setString(idx++, dimMapping.getUnmappedPath());
		psInsert.setString(idx++, dimMapping.getOnUnmapped().name());
		psInsert.setBoolean(idx++, dimMapping.isAutoCreateMapping());
		int count = psInsert.executeUpdate(); 
		if (count != 1) {
			throw new SQLException("Error inserting DimMappingDef " + dimMapping.getKey() + ": Updated " + count + " but expected 1");
		}
		
	}
	
}
