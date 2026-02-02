from pyspark.sql import SparkSession
from datetime import datetime
import json
import time
from pathlib import Path

class SparkPipelineLogger:
    def __init__(self, spark: SparkSession, log_dir="reports/logs"):
        self.spark = spark
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Fichier de log principal
        self.execution_log = self.log_dir / f"execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.metrics_file = self.log_dir / f"metrics_{datetime.now().strftime('%Y%m%d')}.json"
        
        # Collecter les métriques
        self.metrics = {
            "pipeline_start": datetime.now().isoformat(),
            "stages": {},
            "data_quality_checks": []
        }
    
    def log_stage_start(self, stage_name: str):
        """Démarrer une étape avec timestamp"""
        self._write_log(f"🚀 STARTING STAGE: {stage_name}")
        self.metrics["stages"][stage_name] = {
            "start_time": datetime.now().isoformat(),
            "status": "running"
        }
    
    def log_stage_end(self, stage_name: str, df=None):
        """Terminer une étape avec statistiques"""
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.metrics["stages"][stage_name]["start_time"])
        duration = (end_time - start_time).total_seconds()
        
        # Collecter des stats si DataFrame fourni
        stats = {}
        if df:
            try:
                stats = {
                    "row_count": df.count(),
                    "column_count": len(df.columns),
                    "schema": [str(field) for field in df.schema.fields],
                    "size_in_mb": self._estimate_size_mb(df)
                }
            except:
                stats = {"error": "Could not compute statistics"}
        
        self.metrics["stages"][stage_name].update({
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "status": "completed",
            "statistics": stats
        })
        
        self._write_log(f"✅ COMPLETED STAGE: {stage_name} in {duration:.2f}s")
        if stats:
            self._write_log(f"   📊 Stats: {json.dumps(stats, indent=2)}")
    
    def log_data_quality_check(self, check_name: str, result: dict):
        """Logger un contrôle qualité"""
        self.metrics["data_quality_checks"].append({
            "check_name": check_name,
            "timestamp": datetime.now().isoformat(),
            "result": result
        })
        self._write_log(f"🔍 QUALITY CHECK: {check_name} - {json.dumps(result)}")
    
    def log_benchmark(self, operation: str, before: dict, after: dict):
        """Logger un benchmark avant/après optimisation"""
        benchmark_file = self.log_dir / f"benchmark_{operation}_{datetime.now().strftime('%H%M%S')}.json"
        benchmark_data = {
            "operation": operation,
            "timestamp": datetime.now().isoformat(),
            "before": before,
            "after": after,
            "improvement_percent": self._calculate_improvement(before, after)
        }
        
        with open(benchmark_file, 'w') as f:
            json.dump(benchmark_data, f, indent=2)
        
        self._write_log(f"⚡ BENCHMARK: {operation} - {benchmark_data['improvement_percent']:.1f}% improvement")
    
    def save_spark_ui_screenshot(self, stage: str):
        """Capturer des métriques Spark UI (à adapter selon votre setup)"""
        # Exemple: capturer certaines métriques via SparkContext
        sc = self.spark.sparkContext
        
        ui_metrics = {
            "stage": stage,
            "timestamp": datetime.now().isoformat(),
            "application_id": sc.applicationId,
            "executor_memory": sc._conf.get("spark.executor.memory"),
            "shuffle_partitions": sc._conf.get("spark.sql.shuffle.partitions"),
            "rdd_count": len(sc._jsc.sc().getRDDs())
        }
        
        ui_file = self.log_dir / f"spark_ui_{stage}_{datetime.now().strftime('%H%M%S')}.json"
        with open(ui_file, 'w') as f:
            json.dump(ui_metrics, f, indent=2)
    
    def _write_log(self, message: str):
        """Écrire dans le fichier de log"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] {message}\n"
        
        with open(self.execution_log, 'a') as f:
            f.write(log_line)
        
        # Afficher aussi dans la console
        print(log_line.strip())
    
    def _estimate_size_mb(self, df):
        """Estimer la taille d'un DataFrame en MB"""
        try:
            # Approche simple: compter les lignes * colonnes * taille estimée
            row_count = df.count()
            col_count = len(df.columns)
            estimated_bytes = row_count * col_count * 100  # ~100 bytes par cellule
            return estimated_bytes / (1024 * 1024)
        except:
            return 0
    
    def _calculate_improvement(self, before: dict, after: dict):
        """Calculer le pourcentage d'amélioration"""
        if "duration" in before and "duration" in after:
            improvement = ((before["duration"] - after["duration"]) / before["duration"]) * 100
            return improvement
        return 0
    
    def finalize(self):
        """Finaliser et sauvegarder tous les logs"""
        self.metrics["pipeline_end"] = datetime.now().isoformat()
        total_duration = datetime.fromisoformat(self.metrics["pipeline_end"]) - \
                        datetime.fromisoformat(self.metrics["pipeline_start"])
        self.metrics["total_duration_seconds"] = total_duration.total_seconds()
        
        # Sauvegarder les métriques
        with open(self.metrics_file, 'w') as f:
            json.dump(self.metrics, f, indent=2)
        
        self._write_log(f"🎉 PIPELINE COMPLETED in {total_duration.total_seconds():.2f}s")
        self._write_log(f"📁 Metrics saved to: {self.metrics_file}")
        self._write_log(f"📁 Execution log: {self.execution_log}")