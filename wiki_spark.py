from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, regexp_replace, when, expr, split, trim, element_at, broadcast, lit, sum as _sum
)
import glob, os, shutil

def run_spark_merge_logic(nndb_path: str, wiki_dump_path: str, output_path: str = "output.tsv"):
    if not os.path.exists(nndb_path):
        print(f"Error: NNDB file not found at {nndb_path}")
        return False
    if not os.path.exists(wiki_dump_path):
        print(f"Error: Wikipedia dump file not found at {wiki_dump_path}")
        return False

    spark = None
    try:
        print("Initializing Spark session...")
        spark = (
            SparkSession.builder
            .appName("WikiPersonDateExtractorXML_Optimized")
            .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g") 
            .config("spark.executor.memoryOverhead", "4g") # CRITICAL for BZ2/XML    
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
            .config("spark.sql.shuffle.partitions", "40")
            .config("spark.default.parallelism", "40") 
            .config("spark.sql.files.ignoreCorruptFiles", "true")
            .config("spark.sql.adaptive.enabled", "true")  
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        print("Spark session created")

        print(f"Reading NNDB data from {nndb_path}...")
        nndb_df = spark.read.option("header", True).option("sep", "\t").csv(nndb_path)
        total_nndb = nndb_df.count()
        print(f"NNDB records: {total_nndb}")

        nndb_names_df = nndb_df.select("Name").distinct().cache()
        print(f"Unique names to match: {nndb_names_df.count()}")

        print(f"Reading Wikipedia dump from {wiki_dump_path} with filters...")
        wiki_df = (
            spark.read.format("xml")
            .option("rowTag", "page")
            .option("inferSchema", False)
            .load(wiki_dump_path)
        )

        wiki_df = (wiki_df
            .filter(col("ns") == 0)
            .select(col("title").alias("Name"), col("revision.text._VALUE").alias("page_text"))
            .filter(col("page_text").isNotNull())
            .filter(col("page_text").contains("{{Infobox") | col("page_text").contains("{{infobox"))
        )

        # 1. Clean the Wikipedia name column for joining
        wiki_df = wiki_df.withColumn(
            "JoinName",
            trim(regexp_replace(col("Name"), r"\s*\([^)]+\)$", ""))
        )
        
        # 2. DISAMBIGUATION RULE: Keep only the row where the original 'Name'
        wiki_df = wiki_df.filter(col("Name") == col("JoinName"))

        # 3. Rename the NNDB name column for joining
        nndb_names_df = nndb_df.select(col("Name").alias("JoinName")).distinct().cache()

        # 4. Perform the join on the cleaned "JoinName" column
        wiki_df = wiki_df.join(broadcast(nndb_names_df), on="JoinName", how="inner")
        
        # 5. Drop the redundant JoinName column
        wiki_df = wiki_df.drop("JoinName")

        wiki_df = wiki_df.repartition(200).cache()

        wiki_df = wiki_df.withColumn(
            "intro",
            regexp_extract(col("page_text"), r"(?s)(.*?)(==[^=]|$)", 1)
        )

        infobox_pattern = r"(?is)\{\{infobox\s+.*?\n\}\}"

        wiki_df = wiki_df.withColumn(
            "infobox",
            regexp_extract(col("intro"), infobox_pattern, 0)
        )

        wiki_df = wiki_df.filter(col("infobox") != "")

        # 1. Remove the entire Infobox
        wiki_df = wiki_df.withColumn(
            "page_text_clean",
            regexp_replace(col("page_text"), infobox_pattern, "")
        )

        # 2. Extract lead section (before first heading)
        wiki_df = wiki_df.withColumn(
            "intro_clean",
            regexp_extract(col("page_text_clean"), r"(?s)(.*?)(==[^=]|$)", 1)
        )

        # 3. Remove all templates {{...}} (non-greedy, including pipes)
        wiki_df = wiki_df.withColumn(
            "intro_no_templates",
            regexp_replace(col("intro_clean"), r"\{\{[^{}]*\}\}", "")
        )

        # 4. Remove bracketed URLs [http://... Label]
        wiki_df = wiki_df.withColumn(
            "intro_no_templates",
            regexp_replace(col("intro_no_templates"), r"\[https?:\/\/[^\s\]]+\s*([^\]]*)\]", r"\1")
        )

        # 5. Remove wiki links [[...]] keeping label
        wiki_df = wiki_df.withColumn(
            "intro_no_templates",
            regexp_replace(col("intro_no_templates"), r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]", r"\1")
        )

        # 6. Remove remaining citations like [1], [2], etc.
        wiki_df = wiki_df.withColumn(
            "intro_no_templates",
            regexp_replace(col("intro_no_templates"), r"\[\d+\]", "")
        )

        # 7. Remove HTML tags
        wiki_df = wiki_df.withColumn(
            "intro_no_templates",
            regexp_replace(col("intro_no_templates"), r"<.*?>", "")
        )

        # 8. Normalize whitespace
        wiki_df = wiki_df.withColumn(
            "intro_no_templates",
            regexp_replace(col("intro_no_templates"), r"\s+", " ")
        )

        # 9. Extract first sentence only (up to . ! or ?)
        wiki_df = wiki_df.withColumn(
            "WikiDescription",
            trim(regexp_extract(col("intro_no_templates"), r"([^.!?]*[.!?])", 1))
        )

        def clean_field(field):
            return trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(field, r"<.*?>", ""),
                        r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]", r"$1"
                    ),
                    r"\{\{[^{}]*\}\}", ""
                )
            )

        wiki_df = wiki_df.withColumn(
            "birth_raw",
            regexp_extract(col("infobox"), r"(?i)birth[_ ]?date\s*=\s*([^\n|]+)", 1)
        )

        wiki_df = wiki_df.withColumn(
            "WikiBorn",
            when(
                col("birth_raw").rlike(r"\d{4}.*?\d{1,2}.*?\d{1,2}"),
                expr("""
                    concat(
                        regexp_extract(birth_raw, '(\\d{4})', 1),
                        '-',
                        lpad(regexp_extract(birth_raw, '\\d{4}\\D+(\\d{1,2})', 1), 2, '0'),
                        '-',
                        lpad(regexp_extract(birth_raw, '\\d{4}\\D+\\d{1,2}\\D+(\\d{1,2})', 1), 2, '0')
                    )
                """)
            ).otherwise("")
        )

        wiki_df = wiki_df.withColumn(
            "WikiOccupation",
            clean_field(regexp_extract(col("infobox"), r"(?i)occupation\s*=\s*([^\n|]+)", 1))
        )

        wiki_df = wiki_df.withColumn(
            "WikiBirthplace",
            clean_field(regexp_extract(col("infobox"), r"(?i)birth_?place\s*=\s*([^\n|]+)", 1))
        )

        wiki_df = wiki_df.select(
            "Name", "WikiDescription", "WikiBorn", 
            "WikiOccupation", "WikiBirthplace"
        )

        wiki_df = wiki_df.filter(
            (col("WikiBorn") != "") |
            (col("WikiOccupation") != "") |
            (col("WikiBirthplace") != "") |
            (col("WikiDescription") != "")
        )

        matched_count = wiki_df.count()
        print("Joining with NNDB using broadcast join...")
        joined_df = nndb_df.join(broadcast(wiki_df), on="Name", how="left")

        joined_df = joined_df.withColumn("field_updated", lit(0))

        def is_empty(column_name):
            return col(column_name).isNull() | (trim(col(column_name)) == "")

        def is_not_empty(column_name):
            return col(column_name).isNotNull() & (trim(col(column_name)) != "")

        if "Born" in nndb_df.columns:
            joined_df = joined_df.withColumn(
                "field_updated",
                when(
                    is_not_empty("WikiBorn") & 
                    is_not_empty("Born") & 
                    (col("Born") != col("WikiBorn")),
                    col("field_updated") + 1
                ).otherwise(col("field_updated"))
            )
            joined_df = joined_df.withColumn(
                "Born",
                when(
                    is_empty("Born") | 
                    (is_not_empty("WikiBorn") & (col("Born") != col("WikiBorn"))),
                    col("WikiBorn")
                ).otherwise(col("Born"))
            )
        else:
            joined_df = joined_df.withColumnRenamed("WikiBorn", "Born")

        if "Occupation" in nndb_df.columns:
            joined_df = joined_df.withColumn(
                "field_updated",
                when(
                    is_not_empty("WikiOccupation") & 
                    is_not_empty("Occupation") & 
                    (col("Occupation") != col("WikiOccupation")),
                    col("field_updated") + 1
                ).otherwise(col("field_updated"))
            )
            joined_df = joined_df.withColumn(
                "Occupation",
                when(
                    is_empty("Occupation") | 
                    (is_not_empty("WikiOccupation") & (col("Occupation") != col("WikiOccupation"))),
                    col("WikiOccupation")
                ).otherwise(col("Occupation"))
            )
        else:
            joined_df = joined_df.withColumnRenamed("WikiOccupation", "Occupation")

        if "Birthplace" in nndb_df.columns:
            joined_df = joined_df.withColumn(
                "field_updated",
                when(
                    is_not_empty("WikiBirthplace") & 
                    is_not_empty("Birthplace") & 
                    (col("Birthplace") != col("WikiBirthplace")),
                    col("field_updated") + 1
                ).otherwise(col("field_updated"))
            )
            joined_df = joined_df.withColumn(
                "Birthplace",
                when(
                    is_empty("Birthplace") | 
                    (is_not_empty("WikiBirthplace") & (col("Birthplace") != col("WikiBirthplace"))),
                    col("WikiBirthplace")
                ).otherwise(col("Birthplace"))
            )
        else:
            joined_df = joined_df.withColumnRenamed("WikiBirthplace", "Birthplace")

        if "Description" in nndb_df.columns:
            joined_df = joined_df.withColumn(
                "Description",
                when(
                    is_empty("Description"),
                    col("WikiDescription")
                ).otherwise(col("Description"))
            )
        else:
            joined_df = joined_df.withColumnRenamed("WikiDescription", "Description")

        total_fields_updated = joined_df.agg(_sum("field_updated").alias("total")).collect()[0]["total"]

        print(f"Total existing fields updated: {total_fields_updated}")
        print(f"Records matched from Wikipedia: {matched_count}/{total_nndb} ({matched_count/total_nndb*100:.2f}%)")

        for col_name in ["WikiBorn", "WikiOccupation", "WikiDescription", "WikiBirthplace", "field_updated"]:
            if col_name in joined_df.columns:
                joined_df = joined_df.drop(col_name)

        print(f"\nWriting output to {output_path}...")
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        joined_df.coalesce(1).write.mode("overwrite").option("sep", "\t").option("header", True).csv("output_temp")

        part = glob.glob("output_temp/part-*.csv")[0]
        os.rename(part, output_path)
        shutil.rmtree("output_temp")

        print(f"Output written to {output_path}")
        print(f"Final records: {joined_df.count()}")

        wiki_df.unpersist()
        nndb_names_df.unpersist()

    except FileNotFoundError as e:
        print(f"Error: Input file not found. {e}")
        return False
    except Exception as e:
        print(f"An error occurred during Spark processing: {e}")
        return False
    finally:
        if spark:
            print("Stopping Spark session...")
            spark.stop()
            print("Spark session stopped.")
    return True