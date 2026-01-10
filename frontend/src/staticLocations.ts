export type StaticLocation = {
  name: string;
  latitude: number;
  longitude: number;
  type: "facility" | "child_camp";
  address?: string | null;
  addressFull?: string | null;
  note?: string;
};

export const STATIC_LOCATIONS: StaticLocation[] = [
  {
    name: "Adelanto ICE Processing Center (CA)",
    latitude: 34.5591013,
    longitude: -117.4414952,
    type: "facility",
    address: "Adelanto East 10400 Rancho Road | Adelanto West 10250 Rancho Road",
    addressFull: "Adelanto East 10400 Rancho Road | Adelanto West 10250 Rancho Road Adelanto, CA 92301 United States",
    note: null
  },
  {
    name: "South Texas Family Residential Center (Dilley, TX)",
    latitude: 28.6578745,
    longitude: -99.2003862,
    type: "facility",
    address: null,
    addressFull: "300 El Rancho Way Dilley, TX 78017 United States",
    note: null
  },
  {
    name: "Tacoma ICE Processing Center (WA)",
    latitude: 47.24986029999999,
    longitude: -122.4218964,
    type: "facility",
    address: "1623 E J Street, Suite 2",
    addressFull: "1623 E J Street, Suite 2 Tacoma, WA 98421-1615 United States",
    note: null
  },
  {
    name: "Elizabeth Contract Detention Facility (NJ)",
    latitude: 40.6660324,
    longitude: -74.1898667,
    type: "facility",
    address: "625 Evans Street",
    addressFull: "625 Evans Street Elizabeth, NJ 07201 United States",
    note: null
  },
  {
    name: "Irwin County Detention Center (GA)",
    latitude: 31.5925,
    longitude: -83.2557,
    type: "facility",
    address: null,
    addressFull: "132 Cotton Drive Ocilla, GA 31774 United States",
    note: null
  },
  {
    name: "Fort Bliss Emergency Intake Site (TX)",
    latitude: 31.7926,
    longitude: -106.424,
    type: "child_camp",
    address: null,
    addressFull: "Fort Bliss, TX",
    note: "Temporary intake site"
  },
  {
    name: "Homestead Temporary Shelter (FL)",
    latitude: 25.497,
    longitude: -80.486,
    type: "child_camp",
    address: null,
    addressFull: "Homestead, FL",
    note: "Unaccompanied children shelter"
  },
  {
    name: "Carrizo Springs Influx Care Facility (TX)",
    latitude: 28.522,
    longitude: -99.873,
    type: "child_camp",
    address: null,
    addressFull: "Carrizo Springs, TX",
    note: "Unaccompanied children facility"
  },
  {
    name: "Donna Soft-Sided Facility (TX)",
    latitude: 26.1707,
    longitude: -98.0494,
    type: "child_camp",
    address: null,
    addressFull: "Donna, TX",
    note: "Unaccompanied children facility"
  },
];
