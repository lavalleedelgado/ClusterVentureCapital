// import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SaveMode

// Collect the extant view in longform.
val vc_long = spark.table("pld_vc_long")

// Calculate aggregations for employment and financing by MSA.
val vc_aggs = vc_long.
    select(
        $"year",
        $"quarter",
        $"msa_code",
        $"cluster_emp",
        $"cluster_amt"
    ).
    groupBy(
        $"year",
        $"quarter",
        $"msa_code"
    ).
    agg(
        sum($"cluster_emp").as("msa_emp"),
        sum($"cluster_amt").as("msa_amt")
    )

// Collect distinct cluster labels from NAICS correspondence table as an array.
val cluster_labels = spark.table("pld_naics").
    select("cluster_label").
    distinct.
    rdd.
    map(r => r.getString(0)).
    collect()

// Rearrage the view to wideform by joining each cluster to the aggregations.
var vc_wide = cluster_labels.foldLeft(vc_aggs) {
    (vc_tmp, cluster_label) => vc_tmp.
        join(
            vc_long.
            where($"cluster_label" === cluster_label).
            select(
                $"year",
                $"quarter",
                $"msa_code",
                $"cluster_emp".alias(
                    cluster_label.toLowerCase().replace(" ", "_") + "_emp"
                ),
                $"cluster_amt".alias(
                    cluster_label.toLowerCase().replace(" ", "_") + "_amt"
                )
            ),
            Seq("year", "quarter", "msa_code"),
            "left_outer"
        )
}

// Write the relation to Hive.
vc_wide.na.fill(0).write.mode(SaveMode.Overwrite).saveAsTable("pld_vc_wide")

// Close the Spark session.
System.exit(0)
