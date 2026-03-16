from dataclasses import dataclass
from pathlib import Path
import uuid


@dataclass
class PipelineConfig:
    input_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    quarantine_path: str
    category: str = "electronics"
    pipeline_run_id: str = ""

    def __post_init__(self) -> None:
        if not self.pipeline_run_id:
            self.pipeline_run_id = str(uuid.uuid4())

    def ensure_directories(self) -> None:
        for path in [self.bronze_path, self.silver_path, self.gold_path, self.quarantine_path]:
            Path(path).mkdir(parents=True, exist_ok=True)
