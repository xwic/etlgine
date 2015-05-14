/*
 * Copyright (c) NetApp Inc. - All Rights Reserved
 * 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 *  
 */
package de.xwic.etlgine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import de.xwic.etlgine.finalizer.ExecuteSqlFinalizer;
import de.xwic.etlgine.loader.jdbc.SqlDialect;

@RunWith(MockitoJUnitRunner.class)
public class ExecuteSqlFinalizerTest {

	public static final String SHARE_PREFIX = "_sharedConnection.";

	/**
	 * A mock/stub (fake) object that will be used with the transformer
	 */
	@Mock
	private IProcessContext processContext;
	
	@Mock
	private IMonitor monitor;

	private Connection connection;
	private Statement statement;
	private String connectionId = "connectionId";
	private String sql = "update sql";

	@Before
	public void setup() throws Exception {
		connection = mock(Connection.class);
		statement = mock(Statement.class);
		Mockito.when(connection.createStatement()).thenReturn(statement);
		Mockito.when(processContext.getResult()).thenReturn(Result.SUCCESSFULL);
		Mockito.when(processContext.getData(SHARE_PREFIX + connectionId)).thenReturn(connection);
		Mockito.when(processContext.getMonitor()).thenReturn(monitor);
	}

	@After
	public void clear() {
		connection = null;
		statement = null;
	}

	/**
	 * Check if commit is not called if not requested so
	 * 
	 * @throws Exception
	 */
	@Test
	public void onFinishNoCommit() throws Exception {

		Mockito.when(statement.executeUpdate(sql)).thenReturn(1);

		ExecuteSqlFinalizer finalizer = new ExecuteSqlFinalizer(connectionId, sql);

		finalizer.onFinish(processContext);
		verify(connection, never()).commit();
		verify(connection, never()).close();

	}

	/**
	 * Check if commit is called after successful update
	 * 
	 * @throws Exception
	 */
	@Test
	public void onFinishWithCommit() throws Exception {

		String sql = "sql";
		String message = "Updated %d records";
		int countRecords = 1;
		Mockito.when(statement.executeUpdate(sql)).thenReturn(countRecords);

		ExecuteSqlFinalizer finalizer = new ExecuteSqlFinalizer(connectionId, sql, message, true);

		// when
		finalizer.onFinish(processContext);

		verify(statement, times(1)).executeUpdate(sql);
		verify(connection, times(1)).commit();
		verify(connection, times(2)).close();

		assertEquals(String.format(message, countRecords), finalizer.getSuccessMessage());

	}

	/**
	 * Check if roll back is called in case of failed update
	 * 
	 * @throws Exception
	 */
	@Test
	public void onFinishWithRollback() throws Exception {

		String msg = "exception message";
		Mockito.when(statement.executeUpdate(sql)).thenThrow(new RuntimeException(msg));
		Mockito.when(processContext.getResult()).thenReturn(Result.SUCCESSFULL).thenReturn(Result.FAILED);
		Mockito.when(connection.isClosed()).thenReturn(false).thenReturn(false).thenReturn(false).thenReturn(true);
		
		ExecuteSqlFinalizer finalizer = new ExecuteSqlFinalizer(connectionId, sql, true);

		try {
			finalizer.onFinish(processContext);
			fail("an sql exception should have been thrown");
		} catch (ETLException ex) {
			//should have been thrown an exception
			assertEquals(msg,ex.getCause().getMessage());
		}
		verify(connection, times(1)).rollback();
		verify(connection, times(1)).close();

	}

	/**
	 * Check if the update sql is executed in case of failed previous result
	 * 
	 * @throws Exception
	 *
	 */
	@Test
	public void onFinishFailedPrevResult() throws Exception {

		Mockito.when(processContext.getResult()).thenReturn(Result.FAILED);
		Mockito.when(connection.isClosed()).thenReturn(false).thenReturn(false).thenReturn(false).thenReturn(true);
		String sql = "sql";
		ExecuteSqlFinalizer finalizer = new ExecuteSqlFinalizer(connectionId, sql, true);

		finalizer.onFinish(processContext);

		verify(statement, never()).executeUpdate(sql);
		verify(connection, times(1)).rollback();
		verify(connection, times(1)).close();
	}

	/**
	 * No statement shall be executed if the previous result is failed and connection closed
	 * 
	 * @throws Exception
	 */
	@Test
	public void onFinishFailedPrevResultNoRollBackIfClosed() throws Exception {
		Mockito.when(connection.isClosed()).thenReturn(true);
		Mockito.when(processContext.getResult()).thenReturn(Result.FAILED);
		String sql = "sql";
		ExecuteSqlFinalizer finalizer = new ExecuteSqlFinalizer(connectionId, sql, true);
		finalizer.setConnection(connection);
		finalizer.onFinish(processContext);

		verify(statement, never()).executeUpdate(sql);
		verify(connection, never()).rollback();
		verify(connection, never()).close();
	}
	
	@Test
	public void useSameDateSQLServer() throws Exception {
		String expectedDate = "2015-01-12 11:34:30";
		String sqlGetDate = "select getdate()";
		//String sqlGetDate3 = "select GETDATE()";

		ResultSet rs = mock(ResultSet.class);
		Mockito.when(statement.executeQuery(sqlGetDate)).thenReturn(rs);
		//Mockito.when(statement.executeQuery(sqlGetDate3)).thenReturn(rs);
		Mockito.when(rs.next()).thenReturn(true);
		List<String> sqlStatements  = new ArrayList<String>();
		
		ExecuteSqlFinalizer finalizer = new ExecuteSqlFinalizer(connectionId, connectionId ,sqlStatements, false);
		Mockito.when(rs.getDate(1)).thenReturn(
				new java.sql.Date( new SimpleDateFormat(finalizer.getDateFormat()).parse(expectedDate).getTime()));

		finalizer.setUseSameDateForAllStatements(true);
		sqlStatements.add(sqlGetDate);
		finalizer.onFinish(processContext);
		finalizer.getProcessedSqlStatements();
		
		assertEquals(1, finalizer.getProcessedSqlStatements().size());
		assertEquals("select cast('"+expectedDate+"' as datetime)", finalizer.getProcessedSqlStatements().get(0));

	}
	
	@Test
	public void useSameDateComplexQuery() throws Exception {
		String expectedDate = "2015-01-12 11:34:30";
		String sqlGetDate = "select getdate()";
		String sqlWithDate = "INSERT INTO SAP_DASHBOARD_BACKLOG"+
			    "(CASEID, FULLNAME, SYMPTOMTEXT, PRIORITY, NAHIGHPRIORITYDESC, NASUPPORTOFFERINGDESC, NA_RECEIVEDVIA, STATUSTEXT, NA_TEARULES, CASE_AGE,"+
			    	     "ASUP_GROUP_MESSAGE, CREATEDATE_EST, DATECLOSED_EST, LASTUPDATED_EST, NA_SYMPTOMNAME, SERIALNUMBER, CUSTOMERNAME, [Calendar Date],"+ 
			    	    "MEASURETYPE, [KEY], OS_VERSION, SYSTEM_NAME, PRODUCT_ID, DAYS_SINCE_UPDATE, CASE_DESCRIPTION, CASE_TYPE, ASSIGNED_TO, REGION2,"+ 
			    	    "CASE_TAG, IOBJECTID, CASE_TAG2, RESPONSEDATE_EST, REOPEN_FLAG, REOPEN_DATE, BUG_COUNT, EXTREFERENCENO, CTQ_SUBTEAM_NAME," +
			    	    "CTQ_SUPPORT_GEO, CTQ_TSC_FLAG, CTQ_SUBTEAM_GROUP, CTQ_MANAGER, CTQ_ROLE, CTQ_SHIFT, CTQ_LEVEL, CTQ_TEAM, CTQ_SUBTEAM_ID," +
			    	    "TAGCATEGORY1DESC, TAGCATEGORY2DESC, CHANGED_BY, CHANGED_BY_ID, MKT_CLASSIF_DES, MKT_USPS_DES, MKT_CUST_SUP_DES," +
			    	    "MKT_SEGMENT_DES, ASUP_AM, LAST_NETAPP_RESPONSE_DT,DATE_RUN_UTC) "+
			    	"SELECT     SAP_BACKLOG1.CASEID, SAP_PERSON.FULLNAME, SAP_BACKLOG1.SYMPTOMTEXT, SAP_BACKLOG1.PRIORITY, SAP_BACKLOG1.NAHIGHPRIORITYDESC," +
			    	    "SAP_BACKLOG1.NASUPPORTOFFERINGDESC, SAP_BACKLOG1.NA_RECEIVEDVIA, SAP_BACKLOG1.STATUSTEXT, SAP_BACKLOG1.NA_TEARULES,"+
			    	    "CONVERT(DECIMAL(18,9),SAP_BACKLOG1.DATE_RUN_UTC - SAP_BACKLOG1.CREATEDATE) AS CASE_AGE, SAP_CASES.ASUP_GROUP_MESSAGE, SAP_CASES.CREATEDATE_EST, SAP_CASES.DATECLOSED_EST," +
			    	    "DATEADD(HOUR, DATEDIFF(HOUR, GETUTCDATE(), DATEADD(HOUR, 3, GETDATE())), SAP_BACKLOG1.LASTUPDATEDATE) AS LASTUPDATEDATE," +
			    	    "SAP_BACKLOG1.NA_SYMPTOMNAME, SAP_ZINSTALLBASE.SERIALNUMBER, SAP_CUSTOMER.CUSTOMERNAME, SAP_BACKLOG1.DATE_RUN_SHORT AS [Calendar Date]," +
			    	    "'BACKLOG' AS MEASURETYPE, SAP_BACKLOG1.CASEID + N'BACKLOG' + TSCO_DATES.[Calendar Date Code] AS [KEY], SAP_BACKLOG1.OSVERSION, SAP_ZINSTALLBASE.NASYSTEMNAME," +
			    	    "SAP_ZINSTALLBASE.PRODUCTID, CONVERT(DECIMAL(18,9),SAP_BACKLOG1.DATE_RUN_UTC - SAP_BACKLOG1.LASTUPDATEDATE) AS DAYS_SINCE_UPDATE, SAP_BACKLOG1.DESCRIPTION," +
			    	    "SAP_BACKLOG1.CASETYPEDESC, SAP_BACKLOG1.ASSIGNED_TO, SAP_PERSON.REGION, SAP_TAG_POSITIONS.CASE_TAG, SAP_BACKLOG1.IOBJECTID, SAP_TAG_POSITIONS.CASE_TAG2," +
			    	    "SAP_CASES.RESPONSEDATE_EST, SAP_CASES.REOPEN_FLAG, SAP_CASES.REOPEN_DATE, SAP_CASES.BUG_COUNT, SAP_BACKLOG1.EXTREFERENCENO," +
			    	    "SAP_PERSON.SAP_ORGANIZATION_SUBTEAM, SAP_SUBTEAM_MAPPING.SUPPORT_GEO, SAP_SUBTEAM_MAPPING.TSC_FLAG, SAP_SUBTEAM_MAPPING.SUBTEAM_GROUP," +
			    	    "SAP_PERSON.MANAGER, SAP_PERSON.ROLE, SAP_PERSON.SHIFT, SAP_PERSON.[LEVEL], SAP_PERSON.TEAM, SAP_PERSON.SAP_ORGANIZATIONID, SAP_BACKLOG1.TAGCATEGORY1DESC," +
			    	    "SAP_BACKLOG1.TAGCATEGORY2DESC, SAP_CASES.CHANGEDBY, SAP_CASES.CHANGEDBY_ID, SAP_BACKLOG1.MKT_CLASSIF_DES, SAP_BACKLOG1.MKT_USPS_DES," +
			    	    "SAP_BACKLOG1.MKT_CUST_SUP_DES, SAP_BACKLOG1.MKT_SEGMENT_DES, SAP_BACKLOG1.ASUP_AM, SAP_CASES.LAST_NETAPP_RESPONSE_DT,SAP_BACKLOG1.DATE_RUN_UTC "+
			    	"FROM         SAP_ZINSTALLBASE RIGHT OUTER JOIN "+
			    	    "SAP_BACKLOG1 LEFT OUTER JOIN "+
			    	    "TSCO_DATES ON SAP_BACKLOG1.DATE_RUN_SHORT = TSCO_DATES.[Calendar Date] LEFT OUTER JOIN "+
			    	    "SAP_TAG_POSITIONS ON SAP_BACKLOG1.CASEID = SAP_TAG_POSITIONS.CASEID LEFT OUTER JOIN "+
			    	    "SAP_CASES ON SAP_BACKLOG1.CASEID = SAP_CASES.CASEID ON SAP_ZINSTALLBASE.IOBJECTID = SAP_BACKLOG1.IOBJECTID LEFT OUTER JOIN "+
			    	    "SAP_SUBTEAM_MAPPING RIGHT OUTER JOIN "+
			    	    "SAP_PERSON ON SAP_SUBTEAM_MAPPING.SUBTEAM_NAME = SAP_PERSON.SAP_ORGANIZATION_SUBTEAM ON " +
			    	    "SAP_BACKLOG1.ASSIGNED_TO = SAP_PERSON.PERSONID LEFT OUTER JOIN "+
			    	    "SAP_CUSTOMER ON SAP_BACKLOG1.CUSTOMERID = SAP_CUSTOMER.CUSTOMERID "+
			    	"WHERE   (SAP_BACKLOG1.DATE_RUN_SHORT >= dbo.DateOnly(getDate())-3);";
		String expectedSqlGetDate = "INSERT INTO SAP_DASHBOARD_BACKLOG"+
			    "(CASEID, FULLNAME, SYMPTOMTEXT, PRIORITY, NAHIGHPRIORITYDESC, NASUPPORTOFFERINGDESC, NA_RECEIVEDVIA, STATUSTEXT, NA_TEARULES, CASE_AGE,"+
			    	     "ASUP_GROUP_MESSAGE, CREATEDATE_EST, DATECLOSED_EST, LASTUPDATED_EST, NA_SYMPTOMNAME, SERIALNUMBER, CUSTOMERNAME, [Calendar Date],"+ 
			    	    "MEASURETYPE, [KEY], OS_VERSION, SYSTEM_NAME, PRODUCT_ID, DAYS_SINCE_UPDATE, CASE_DESCRIPTION, CASE_TYPE, ASSIGNED_TO, REGION2,"+ 
			    	    "CASE_TAG, IOBJECTID, CASE_TAG2, RESPONSEDATE_EST, REOPEN_FLAG, REOPEN_DATE, BUG_COUNT, EXTREFERENCENO, CTQ_SUBTEAM_NAME," +
			    	    "CTQ_SUPPORT_GEO, CTQ_TSC_FLAG, CTQ_SUBTEAM_GROUP, CTQ_MANAGER, CTQ_ROLE, CTQ_SHIFT, CTQ_LEVEL, CTQ_TEAM, CTQ_SUBTEAM_ID," +
			    	    "TAGCATEGORY1DESC, TAGCATEGORY2DESC, CHANGED_BY, CHANGED_BY_ID, MKT_CLASSIF_DES, MKT_USPS_DES, MKT_CUST_SUP_DES," +
			    	    "MKT_SEGMENT_DES, ASUP_AM, LAST_NETAPP_RESPONSE_DT,DATE_RUN_UTC) "+
			    	"SELECT     SAP_BACKLOG1.CASEID, SAP_PERSON.FULLNAME, SAP_BACKLOG1.SYMPTOMTEXT, SAP_BACKLOG1.PRIORITY, SAP_BACKLOG1.NAHIGHPRIORITYDESC," +
			    	    "SAP_BACKLOG1.NASUPPORTOFFERINGDESC, SAP_BACKLOG1.NA_RECEIVEDVIA, SAP_BACKLOG1.STATUSTEXT, SAP_BACKLOG1.NA_TEARULES,"+
			    	    "CONVERT(DECIMAL(18,9),SAP_BACKLOG1.DATE_RUN_UTC - SAP_BACKLOG1.CREATEDATE) AS CASE_AGE, SAP_CASES.ASUP_GROUP_MESSAGE, SAP_CASES.CREATEDATE_EST, SAP_CASES.DATECLOSED_EST," +
			    	    "DATEADD(HOUR, DATEDIFF(HOUR, GETUTCDATE(), DATEADD(HOUR, 3, cast('2015-01-12 11:34:30' as datetime))), SAP_BACKLOG1.LASTUPDATEDATE) AS LASTUPDATEDATE," +
			    	    "SAP_BACKLOG1.NA_SYMPTOMNAME, SAP_ZINSTALLBASE.SERIALNUMBER, SAP_CUSTOMER.CUSTOMERNAME, SAP_BACKLOG1.DATE_RUN_SHORT AS [Calendar Date]," +
			    	    "'BACKLOG' AS MEASURETYPE, SAP_BACKLOG1.CASEID + N'BACKLOG' + TSCO_DATES.[Calendar Date Code] AS [KEY], SAP_BACKLOG1.OSVERSION, SAP_ZINSTALLBASE.NASYSTEMNAME," +
			    	    "SAP_ZINSTALLBASE.PRODUCTID, CONVERT(DECIMAL(18,9),SAP_BACKLOG1.DATE_RUN_UTC - SAP_BACKLOG1.LASTUPDATEDATE) AS DAYS_SINCE_UPDATE, SAP_BACKLOG1.DESCRIPTION," +
			    	    "SAP_BACKLOG1.CASETYPEDESC, SAP_BACKLOG1.ASSIGNED_TO, SAP_PERSON.REGION, SAP_TAG_POSITIONS.CASE_TAG, SAP_BACKLOG1.IOBJECTID, SAP_TAG_POSITIONS.CASE_TAG2," +
			    	    "SAP_CASES.RESPONSEDATE_EST, SAP_CASES.REOPEN_FLAG, SAP_CASES.REOPEN_DATE, SAP_CASES.BUG_COUNT, SAP_BACKLOG1.EXTREFERENCENO," +
			    	    "SAP_PERSON.SAP_ORGANIZATION_SUBTEAM, SAP_SUBTEAM_MAPPING.SUPPORT_GEO, SAP_SUBTEAM_MAPPING.TSC_FLAG, SAP_SUBTEAM_MAPPING.SUBTEAM_GROUP," +
			    	    "SAP_PERSON.MANAGER, SAP_PERSON.ROLE, SAP_PERSON.SHIFT, SAP_PERSON.[LEVEL], SAP_PERSON.TEAM, SAP_PERSON.SAP_ORGANIZATIONID, SAP_BACKLOG1.TAGCATEGORY1DESC," +
			    	    "SAP_BACKLOG1.TAGCATEGORY2DESC, SAP_CASES.CHANGEDBY, SAP_CASES.CHANGEDBY_ID, SAP_BACKLOG1.MKT_CLASSIF_DES, SAP_BACKLOG1.MKT_USPS_DES," +
			    	    "SAP_BACKLOG1.MKT_CUST_SUP_DES, SAP_BACKLOG1.MKT_SEGMENT_DES, SAP_BACKLOG1.ASUP_AM, SAP_CASES.LAST_NETAPP_RESPONSE_DT,SAP_BACKLOG1.DATE_RUN_UTC "+
			    	"FROM         SAP_ZINSTALLBASE RIGHT OUTER JOIN "+
			    	    "SAP_BACKLOG1 LEFT OUTER JOIN "+
			    	    "TSCO_DATES ON SAP_BACKLOG1.DATE_RUN_SHORT = TSCO_DATES.[Calendar Date] LEFT OUTER JOIN "+
			    	    "SAP_TAG_POSITIONS ON SAP_BACKLOG1.CASEID = SAP_TAG_POSITIONS.CASEID LEFT OUTER JOIN "+
			    	    "SAP_CASES ON SAP_BACKLOG1.CASEID = SAP_CASES.CASEID ON SAP_ZINSTALLBASE.IOBJECTID = SAP_BACKLOG1.IOBJECTID LEFT OUTER JOIN "+
			    	    "SAP_SUBTEAM_MAPPING RIGHT OUTER JOIN "+
			    	    "SAP_PERSON ON SAP_SUBTEAM_MAPPING.SUBTEAM_NAME = SAP_PERSON.SAP_ORGANIZATION_SUBTEAM ON " +
			    	    "SAP_BACKLOG1.ASSIGNED_TO = SAP_PERSON.PERSONID LEFT OUTER JOIN "+
			    	    "SAP_CUSTOMER ON SAP_BACKLOG1.CUSTOMERID = SAP_CUSTOMER.CUSTOMERID "+
			    	"WHERE   (SAP_BACKLOG1.DATE_RUN_SHORT >= dbo.DateOnly(cast('2015-01-12 11:34:30' as datetime))-3);";
		ResultSet rs = mock(ResultSet.class);
		Mockito.when(statement.executeQuery(sqlGetDate)).thenReturn(rs);
		Mockito.when(rs.next()).thenReturn(true);
		List<String> sqlStatements  = new ArrayList<String>();
		
		ExecuteSqlFinalizer finalizer = new ExecuteSqlFinalizer(connectionId, connectionId ,sqlStatements, false);
		Mockito.when(rs.getDate(1)).thenReturn(
				new java.sql.Date( new SimpleDateFormat(finalizer.getDateFormat()).parse(expectedDate).getTime()));

		finalizer.setUseSameDateForAllStatements(true);
		sqlStatements.add(sqlWithDate);
		finalizer.onFinish(processContext);
		finalizer.getProcessedSqlStatements();
		
		assertEquals(1, finalizer.getProcessedSqlStatements().size());
		assertEquals(expectedSqlGetDate, finalizer.getProcessedSqlStatements().get(0));

	}
	
	
	
	@Test
	public void useSameDateOracle() throws Exception {
		String expectedDate = "2015-01-12 11:34:30";
		String sqlGetDate = "select systimestamp from dual";

		ResultSet rs = mock(ResultSet.class);
		Mockito.when(statement.executeQuery(sqlGetDate)).thenReturn(rs);
		Mockito.when(rs.next()).thenReturn(true);
		List<String> sqlStatements  = new ArrayList<String>();
		
		ExecuteSqlFinalizer finalizer = new ExecuteSqlFinalizer(connectionId, connectionId ,sqlStatements, false);
		Mockito.when(rs.getDate(1)).thenReturn(
				new java.sql.Date( new SimpleDateFormat(finalizer.getDateFormat()).parse(expectedDate).getTime()));

		finalizer.setUseSameDateForAllStatements(true);
		finalizer.setSqlDialect(SqlDialect.ORACLE);
		sqlStatements.add(sqlGetDate);
		finalizer.onFinish(processContext);
		finalizer.getProcessedSqlStatements();
		
		assertEquals(1, finalizer.getProcessedSqlStatements().size());
		assertEquals("select to_timestamp('"+expectedDate+"','YYYY-MM-DD HH24:MI:SS') from dual", finalizer.getProcessedSqlStatements().get(0));
		

	}

}
