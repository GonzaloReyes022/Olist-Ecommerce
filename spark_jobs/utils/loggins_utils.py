"""
Configuración de logging para el pipeline.

Buenas prácticas de logging en Data Engineering:
1. Logs estructurados (fáciles de parsear)
2. Niveles apropiados (DEBUG, INFO, WARNING, ERROR)
3. Contexto útil (batch_id, record_count, etc.)
4. Rotación de archivos para no llenar disco
"""
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import json


class StructuredFormatter(logging.Formatter):
    """
    Formatter que produce logs en formato JSON estructurado.

    Beneficios:
    - Fácil de parsear por herramientas como ELK, Splunk
    - Campos consistentes
    - Metadata adicional
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line_no": record.lineno,
            "process_id": record.process,
            "thread_id": record.thread,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        extra_fields = getattr(record, "extra_fields", None)
        if extra_fields is not None:
            log_data.update(extra_fields)
            
        return json.dumps(log_data)
    
class PipelineLogger:
    """
    Logger especializado para pipelines de datos.

    Provee métodos convenientes para loggear eventos comunes
    con contexto relevante.
    """

    def __init__(self, name : str, log_dir : Optional[Path] = None):
        self.logger = logging.getLogger(name)
        self._setup_handlers(log_dir)

    def _setup_handlers(self, log_dir : Optional[Path] = None):
        """Configura handlers de logging."""
        # Evitar duplicar handlers
        if self.logger.handlers:
            return
        #cacha todo
        self.logger.setLevel(logging.DEBUG)

        # Handler de consola (formato legible)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

        # Handler de archivo (formato JSON)
        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(StructuredFormatter())
            self.logger.addHandler(file_handler)    


    def start_batch(self, batch_id: str, source: str) -> None:
        """Loggea inicio de un batch."""
        self.logger.info(
            f"Batch iniciado | batch_id={batch_id} | source={source}"
        )

    def end_batch(
        self,
        batch_id: str,
        status: str,
        records_processed: int,
        duration_seconds: float
    ) -> None:
        """Loggea fin de un batch."""
        self.logger.info(
            f"Batch finalizado | batch_id={batch_id} | status={status} | "
            f"records={records_processed} | duration={duration_seconds:.2f}s"
        )

    def record_count(self, stage: str, count: int) -> None:
        """Loggea conteo de registros en una etapa."""
        self.logger.info(f"Record count | stage={stage} | count={count}")

    def validation_result(
        self,
        check_name: str,
        passed: bool,
        details: Optional[str] = None
    ) -> None:
        """Loggea resultado de validación."""
        status = "PASSED" if passed else "FAILED"
        level = logging.INFO if passed else logging.WARNING

        msg = f"Validation {status} | check={check_name}"
        if details:
            msg += f" | details={details}"

        self.logger.log(level, msg)

    def quality_metrics(self, metrics: dict) -> None:
        """Loggea métricas de calidad de datos."""
        self.logger.info(f"Quality metrics | {metrics}")

    def error(self, message: str, exception: Optional[Exception] = None) -> None:
        """Loggea un error."""
        if exception:
            self.logger.error(message, exc_info=True)
        else:
            self.logger.error(message)

    def warning(self, message: str) -> None:
        """Loggea una advertencia."""
        self.logger.warning(message)

    def info(self, message: str) -> None:
        """Loggea información general."""
        self.logger.info(message)

    def debug(self, message: str) -> None:
        """Loggea información de debug."""
        self.logger.debug(message)

def get_logger(name:str) -> PipelineLogger:
    """
    Factory function para obtener un logger configurado.

    Args:
        name: Nombre del logger (típicamente __name__)

    Returns:
        PipelineLogger configurado

    Ejemplo:
        >>> logger = get_logger(__name__)
        >>> logger.start_batch("20240115_001", "orders.csv")
        >>> logger.record_count("bronze", 10000)
    """
    from spark_jobs.config.settings import config
    log_dir = config.paths.root / "logs"
    return PipelineLogger(name, log_dir)

