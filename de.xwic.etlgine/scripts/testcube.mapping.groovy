/**
 * Ensure that the specified dimensions exist.
 */

mapper.addMapping "Name"
def mapping = mapper.addMapping("Area")
mapping.autoCreate = false

def mm = mapper.addMeasure ("Bookings", "Booking")
//mm.fixedValue = 1