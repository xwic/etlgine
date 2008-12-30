/**
 * Ensure that the specified dimensions exist.
 */

util.initFromDatabase "default"
 
util.ensureDimension "Name"
util.ensureDimension "Area"



util.ensureElement "Area", "EMEA"
util.ensureElement "Area", "EMEA/HQ"
util.ensureElement "Area", "EMEA/Germany"
util.ensureElement "Area", "EMEA/UK"
util.ensureElement "Area", "EMEA/France"

util.ensureMeasure "Bookings"
util.ensureMeasure "Plan"


util.ensureCube "Test", ["Name", "Area"], ["Bookings"]

