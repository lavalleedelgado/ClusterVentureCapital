import org.apache.spark.sql.SaveMode

// Collect extant views.
val view_by_msa_clutser = spark.table("pld_venture_capital_tmp")
val msa_labels = spark.table("pld_msa")

// Calculate employment and financing by MSA.
val view_by_msa = view_by_msa_clutser.
    select(
        view_by_msa_clutser("year"),
        view_by_msa_clutser("quarter"),
        view_by_msa_clutser("msa_code"),
        view_by_msa_clutser("cluster_emp"),
        view_by_msa_clutser("cluster_amt")
    ).
    groupBy(
        view_by_msa_clutser("year"),
        view_by_msa_clutser("quarter"),
        view_by_msa_clutser("msa_code")
    ).
    agg(
        sum(view_by_msa_clutser("cluster_emp")).as("msa_emp"),
        sum(view_by_msa_clutser("cluster_amt")).as("msa_amt")
    )

// Join the granular and aggregate views with MSA labels.
val venture_capital = view_by_msa_clutser.
    join(view_by_msa, Seq("year", "quarter", "msa_code")).
    join(msa_labels, Seq("msa_code"))

// Write the relation to Hive.
venture_capital.write.mode(SaveMode.Overwrite).saveAsTable("pld_venture_capital")

// Close the Spark session.
System.exit(0)
