/**
 * 
 */
package de.xwic.etlgine;

/**
 * @author Developer
 *
 */
public class TestRndSource implements ISource {

	private int entries = 10000;
	
	/**
	 * Instantiate a random source of 10000 entries.
	 */
	public TestRndSource() {
		
	}
	
	/**
	 * Instantiate a random source with [entries] entries.
	 * @param entries
	 */
	public TestRndSource(int entries) {
		this.entries = entries;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ISource#getName()
	 */
	public String getName() {
		return "Test Random Source";
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ISource#isAvailable()
	 */
	public boolean isAvailable() {
		return true;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ISource#isOptional()
	 */
	public boolean isOptional() {
		return false;
	}

	/**
	 * @return the entries
	 */
	public int getEntries() {
		return entries;
	}

	/**
	 * @param entries the entries to set
	 */
	public void setEntries(int entries) {
		this.entries = entries;
	}

}
