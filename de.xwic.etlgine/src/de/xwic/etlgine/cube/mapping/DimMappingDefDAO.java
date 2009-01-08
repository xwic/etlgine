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
public class DimMappingDefDAO {

	private Connection connection;
	private PreparedStatement psInsert;
	private PreparedStatement psUpdate;
	
	/**
	 * @param connection
	 * @throws SQLException 
	 */
	public DimMappingDefDAO(Connection connection) throws SQLException {
		super();
		this.connection = connection;
		
		psInsert = connection.prepareStatement("INSERT INTO [XCUBE_DIMMAP] (DimMapKey, Description, DimensionKey, UnmappedPath, OnUnmapped) VALUES (?, ?, ?, ?, ?)");
		psUpdate = connection.prepareStatement("UPDATE [XCUBE_DIMMAP] SET Description=?, DimensionKey=?, UnmappedPath=?, OnUnmapped=? WHERE DimMapKey = ?");
		
	}

	/**
	 * Returns the list of DimMappingDefs.
	 * @return
	 * @throws SQLException
	 */
	public List<DimMappingDef> listMappings() throws SQLException {
		
		List<DimMappingDef> list = new ArrayList<DimMappingDef>(); 
		
		Statement stmt = connection.createStatement();
		String sql = "SELECT [DimMapKey], [Description], [DimensionKey], [UnmappedPath], [OnUnmapped] FROM XCUBE_DIMMAP";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			DimMappingDef dmd = new DimMappingDef();
			dmd.setKey(rs.getString("DimMapKey"));
			dmd.setDescription(rs.getString("Description"));
			dmd.setDimensionKey(rs.getString("DimensionKey"));
			dmd.setUnmappedPath(rs.getString("UnmappedPath"));
			dmd.setOnUnmapped(DimMappingDef.Action.valueOf(rs.getString("OnUnmapped")));
			list.add(dmd);
		}
		rs.close();
		stmt.close();
		
		return list;
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
		int count = psInsert.executeUpdate(); 
		if (count != 1) {
			throw new SQLException("Error inserting DimMappingDef " + dimMapping.getKey() + ": Updated " + count + " but expected 1");
		}
		
	}
	
}
