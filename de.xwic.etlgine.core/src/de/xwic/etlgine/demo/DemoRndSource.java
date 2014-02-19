package de.xwic.etlgine.demo;

import de.xwic.etlgine.ISource;

/**
 * Used in test cases and the demo jobs.
 */
public class DemoRndSource implements ISource {

    private int entries = 10000;

    /**
     * Instantiate a random source of 10000 entries.
     */
    public DemoRndSource() {

    }

    /**
     * Instantiate a random source with [entries] entries.
     * @param entries
     */
    public DemoRndSource(int entries) {
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
