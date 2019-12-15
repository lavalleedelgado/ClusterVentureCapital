namespace java pld.IngestExemptOfferings

struct ExemptOffering {
	1: required i64 cik;
    2: required string entity;
	3: required i16 census_year;
	4: required i16 year;
	5: required i8 month;
	6: required i8 day;
	7: required string zip_code;
	8: required string cluster_label;
	9: required double amount;
}

