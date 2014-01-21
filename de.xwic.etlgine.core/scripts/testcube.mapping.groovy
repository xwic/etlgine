/**
 * Ensure that the specified dimensions exist.
 */

mapper.addMapping "Name"
def mapping = mapper.addMapping("Area")
mapping.autoCreate = false
mapping.addElementMapping "EMEA/HQ", "EMEA"
mapping.addElementMapping "EMEA/Germany", "^Ger.*", true
mapping.addElementMapping "EMEA/UK", "UK"
mapping.addElementMapping "EMEA/France", "France"

def mm = mapper.addMeasure ("Bookings", "Booking")
//mm.fixedValue = 1