/**
 * Ensure that the specified dimensions exist.
 */

util.initFromDatabase "etlpooldb"
 
util.ensureDimension "Name"
util.ensureDimension "Area"

util.ensureElement "Name", "Florian"

util.ensureElement "Area", "EMEA"
util.ensureElement "Area", "EMEA/HQ"
util.ensureElement "Area", "EMEA/Germany"
util.ensureElement "Area", "EMEA/UK"
util.ensureElement "Area", "EMEA/France"

util.ensureMeasure "Bookings"
def mePlan = util.ensureMeasure ("Plan");


def cube = util.ensureCube("Test", ["Name", "Area"], ["Bookings", "Plan"]);
def key = cube.createKey("");
cube.setCellValue(key, mePlan, 100000.0d)

