/**
 * Ensure that the specified dimensions exist.
 */
util.ensureDimension "Name"
util.ensureDimension "Area"
util.ensureElement "Area", "EMEA"
util.ensureElement "Area", "EMEA/HQ"
util.ensureElement "Area", "EMEA/Germany"
util.ensureElement "Area", "EMEA/UK"
util.ensureElement "Area", "EMEA/France"

util.ensureMeasure "Bookings"

util.ensureCube "Test", ["Name", "Area"], ["Bookings"]

