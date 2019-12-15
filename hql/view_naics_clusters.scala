import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

// Identify the NAICS data.
val naics = spark.table("pld_naics_csv")

// Assign cluster labels.
val naics_clusters = naics._
    withColumn("cluster_label",
        when(substring(naics("naics_code"), 1, 2) === "11", "Agriculture").
        when(substring(naics("naics_code"), 1, 3) === "211", "Oil and Gas").
        when(substring(naics("naics_code"), 1, 4) === "2122", "Coal Mining").
        when(substring(naics("naics_code"), 1, 4) === "2211", "Electric Utilities").
        when(substring(naics("naics_code"), 1, 6) === "221112", "Oil and Gas").
        when(substring(naics("naics_code"), 1, 4) === "2212", "Other Energy").
        when(substring(naics("naics_code"), 1, 2) === "23", "Construction").
        when(substring(naics("naics_code"), 1, 1) === "3", "Manufacturing").
        when(substring(naics("naics_code"), 1, 4) === "3254", "Pharmaceuticals").
        when(substring(naics("naics_code"), 1, 3) === "334", "Computers").
        when(substring(naics("naics_code"), 1, 6) === "336411", "Airlines and Airports").
        when(substring(naics("naics_code"), 1, 2) === "44", "Retailing").
        when(substring(naics("naics_code"), 1, 2) === "45", "Retailing").
        when(substring(naics("naics_code"), 1, 3) === "481", "Airlines and Airports").
        when(substring(naics("naics_code"), 1, 4) === "4881", "Airlines and Airports").
        when(substring(naics("naics_code"), 1, 4) === "5112", "Computers").
        when(substring(naics("naics_code"), 1, 3) === "515", "Telecommunications").
        when(substring(naics("naics_code"), 1, 3) === "517", "Telecommunications").
        when(substring(naics("naics_code"), 1, 2) === "52", "Other Banking and Financial Services").
        when(substring(naics("naics_code"), 1, 5) === "52211", "Commercial Banking").
        when(substring(naics("naics_code"), 1, 6) === "522292", "REITS and Finance").
        when(substring(naics("naics_code"), 1, 3) === "523", "Investing").
        when(substring(naics("naics_code"), 1, 5) === "52311", "Investment Banking").
        when(substring(naics("naics_code"), 1, 3) === "524", "Insurance").
        when(substring(naics("naics_code"), 1, 6) === "524114", "Health Insurance").
        when(substring(naics("naics_code"), 1, 3) === "525", "Pooled Investment Fund").
        when(substring(naics("naics_code"), 1, 2) === "53", "Other Real Estate").
        when(substring(naics("naics_code"), 1, 3) === "531", "Commerical").
        when(substring(naics("naics_code"), 1, 5) === "53111", "Residential").
        when(substring(naics("naics_code"), 1, 6) === "531311", "Residential").
        when(substring(naics("naics_code"), 1, 3) === "541", "Business Services").
        when(substring(naics("naics_code"), 1, 4) === "5415", "Computers").
        when(substring(naics("naics_code"), 1, 5) === "54162", "Environmental Services").
        when(substring(naics("naics_code"), 1, 5) === "54169", "Other Technology").
        when(substring(naics("naics_code"), 1, 6) === "541711", "Biotechnology").
        when(substring(naics("naics_code"), 1, 4) === "5615", "Tourism and Travel Services").
        when(substring(naics("naics_code"), 1, 2) === "62", "Other Health Care").
        when(substring(naics("naics_code"), 1, 4) === "6211", "Hospitals and Physicians").
        when(substring(naics("naics_code"), 1, 3) === "622", "Hospitals and Physicians").
        when(substring(naics("naics_code"), 1, 4) === "7211", "Lodging and Conventions").
        when(substring(naics("naics_code"), 1, 3) === "722", "Restaurants").
        otherwise("Other")
    )

// Write the relation to Hive.
naics_clusters.write.mode(SaveMode.Overwrite).saveAsTable("pld_naics")

// Close the Spark session.
System.exit(0)
