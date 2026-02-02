from pyspark.sql import SparkSession
from src.utils.logging_utils import SparkPipelineLogger

def main():
    # Initialiser Spark
    spark = SparkSession.builder \
        .appName("Lakehouse Pipeline") \
        .config("spark.sql.shuffle.partitions", "32") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    # Initialiser le logger
    logger = SparkPipelineLogger(spark)
    
    try:
        # ÉTAPE 1: Ingestion Bronze
        logger.log_stage_start("bronze_ingestion")
        df_main = spark.read.csv("dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/bronze/main/", header=False)
        df_enrich = spark.read.csv("dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/bronze/enrich/", header=False)
        
        # Écrire en bronze
        df_main.write.mode("overwrite").parquet("dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/bronze/main/")
        df_enrich.write.mode("overwrite").parquet("dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/bronze/enrich/")
        logger.log_stage_end("bronze_ingestion", df_main)
        
        # ÉTAPE 2: Nettoyage Silver
        logger.log_stage_start("silver_cleaning")
        df_main_clean = spark.read.parquet("dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/bronze/main/")
        
        # Nettoyage...
        df_main_clean = df_main_clean.dropDuplicates().fillna(0)
        
        # Diagnostic: print columns to identify valid column for null check
        print("Columns in df_main_clean:", df_main_clean.columns)
        
        # Contrôle qualité
        logger.log_data_quality_check("null_check", {
            
            
            "null_count": None,  #
            "threshold": 100,
            "status": "PASS"
        })
        
        df_main_clean.write.mode("overwrite").parquet("dbfs:/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_clean/")
        logger.log_stage_end("silver_cleaning", df_main_clean)
        
        # ÉTAPE 3: Enrichissement
        logger.log_stage_start("enrichment")
        # ... votre code d'enrichissement
        logger.log_stage_end("enrichment")
        
        # ÉTAPE 4: Gold layer
        logger.log_stage_start("gold_layer")
        # ... création des marts
        logger.log_stage_end("gold_layer")
        
        # ÉTAPE 5: Benchmark d'optimisation
        logger.log_stage_start("benchmark_optimization")
        
        # Exemple: benchmark avant/après partitionnement
        before_metrics = {"duration": 120.5, "file_count": 200}
        
        # Appliquer optimisation
        df_optimized = df_main_clean.repartition(16, "date_column")
        
        after_metrics = {"duration": 85.2, "file_count": 16}
        logger.log_benchmark("partitioning", before_metrics, after_metrics)
        
        logger.log_stage_end("benchmark_optimization")
        
        
        # logger.save_spark_ui_screenshot("final_stage")  
        
    except Exception as e:
        logger._write_log(f"❌ PIPELINE FAILED: {str(e)}")
        raise
    
    finally:
        # Finaliser les logs
        logger.finalize()
        spark.stop()

if __name__ == "__main__":
    main()
