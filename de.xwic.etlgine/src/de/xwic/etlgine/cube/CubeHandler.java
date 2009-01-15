/**
 * 
 */
package de.xwic.etlgine.cube;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.cube.DataPoolManagerFactory;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDataPoolManager;
import de.xwic.cube.StorageException;
import de.xwic.cube.storage.impl.FileDataPoolStorageProvider;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.loader.cube.DataPoolInitializer;
import de.xwic.etlgine.server.ServerContext;

/**
 * Handles cubes.
 * @author lippisch
 */
public class CubeHandler {

	public static final String PROPERTY_DATAPOOLS = "datapools";

	protected final Log log = LogFactory.getLog(getClass());
	
	private final IContext context;
	private Map<String, IDataPoolManager> dataPoolManagerMap = new HashMap<String, IDataPoolManager>(); 

	/**
	 * Constructor.
	 * @param context
	 */
	public CubeHandler(IContext context) {
		this.context = context;
		if (context.getData(IContext.DATA_CUBEHANDLER) != null) {
			throw new IllegalStateException("The context already contains a CubeHandler!");
		}
		loadDataPools();
		context.setData(IContext.DATA_CUBEHANDLER, this);
	}
	
	/**
	 * Returns the cube handler associated with the specified context. If the context
	 * does not contain a CubeHandler, a new one is instantiated.
	 * @param context
	 * @return
	 */
	public static CubeHandler getCubeHandler(IContext context) {
		CubeHandler handler;
		if (context.getData(IContext.DATA_CUBEHANDLER) != null) {
			handler = (CubeHandler)context.getData(IContext.DATA_CUBEHANDLER);
		} else {
			handler = new CubeHandler(context);
		}
		return handler;
	}
	
	/**
	 * 
	 */
	private void loadDataPools() {
		
		String pools = context.getProperty(PROPERTY_DATAPOOLS, null);
		if(pools != null)  {
			
			StringTokenizer stk = new StringTokenizer(pools, ",; ");
			while (stk.hasMoreTokens()) {
				String poolKey = stk.nextToken();
				
				String path = context.getProperty(poolKey + ".datapool.path", null);
				
				if (path != null) {
					
					File fRoot = new File(context.getProperty(IContext.PROPERTY_ROOTPATH, "."));
					File fDP = new File(fRoot, path);
					if (fDP.exists()) {
						FileDataPoolStorageProvider storageProvider = new FileDataPoolStorageProvider(fDP);
						IDataPoolManager dpMngr = DataPoolManagerFactory.createDataPoolManager(storageProvider);
						dataPoolManagerMap.put(poolKey, dpMngr);
						log.info("DataPoolManager with the key " + poolKey + " loaded.");
					} else {
						log.warn("The path to the DataPoolManager with the key " + poolKey + " does not exist: " + fDP.getAbsolutePath());
					} 
				} else {
					log.warn("DataPoolManager with the key " + poolKey + " has no path specified.");
				}
				
			}
			
		}
		
	}
	
	/**
	 * Returns the DataPoolManager with the specified key.
	 * @param key
	 * @return
	 */
	public IDataPoolManager getDataPoolManager(String key) {
		return dataPoolManagerMap.get(key);
	}
	
	/**
	 * Returns a collection of loaded data pool managers.
	 * @return
	 */
	public Collection<String> getDataPoolManagerKeys() {
		return dataPoolManagerMap.keySet();
	}

	/**
	 * Open the DataPool with the specified name. The key and path is read from
	 * the context using the dataPoolName. If an initializer script is specified,
	 * it is executed to verify the dataPool integrity.
	 * @param context
	 * @param dataPoolManagerKey
	 * @return
	 */
	public synchronized IDataPool openDataPool(String dataPoolManagerKey) throws ETLException {
		
		String key = context.getProperty(dataPoolManagerKey + ".datapool.key", null);
		
		if (key != null) {
			IDataPoolManager dpm = getDataPoolManager(dataPoolManagerKey);
			try {
				boolean firstLoad = !dpm.containsDataPool(key) || !dpm.isDataPoolLoaded(key);
				IDataPool dataPool;
				if (dpm.containsDataPool(key)) {
					if (firstLoad) {
						log.info("Loading DataPool " + key);
					}
					dataPool = dpm.getDataPool(key);
				} else {
					dataPool = dpm.createDataPool(key);
				}
				
				if (firstLoad) {
					log.info("Initializing DataPool");
					String initScript = context.getProperty(dataPoolManagerKey + ".datapool.initScript", null);
					if (initScript != null) {
						String path = context.getProperty(dataPoolManagerKey + ".datapool.path", null);
						File fRoot = new File(context.getProperty(ServerContext.PROPERTY_ROOTPATH, "."));
						File fDP = new File(fRoot, path);
						File fScript = new File(fDP, initScript);
						if (fScript.exists()) {
							DataPoolInitializer dpInit = new DataPoolInitializer(context, fScript);
							try {
								dpInit.verify(dataPool);
							} catch (Exception e) {
								log.error("Error initializing DataPool: " + e, e);
								dpm.releaseDataPool(dataPool); // release
								throw new ETLException("Error initializing DataPool: " + e, e);
							}
						} else {
							log.warn("Init Script: " + initScript + " does not exist.");
						}

					}

				}
				
				return dataPool;
			} catch (StorageException e) {
				throw new ETLException("Error opening DataPool " + dataPoolManagerKey + " / " + key);
			}
		}
		throw new ETLException("A DataPool for this manager is not available.");
	}

	/**
	 * Returns the connection name for the syncTables of the specified dataPoolKey.
	 * @param dataPoolKey
	 * @return
	 */
	public String getConnectionName(String dataPoolKey) {
		return context.getProperty(dataPoolKey + ".datapool.syncTables.connection");
	}
	
}
