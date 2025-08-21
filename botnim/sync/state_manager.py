"""
Checkpoint/restore state manager for sync runs.

Stores per-source checkpoints in a small JSONL log for fast recovery after partial failures.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from ..config import get_logger


logger = get_logger(__name__)


@dataclass
class Checkpoint:
    source_id: str
    stage: str  # e.g., discover, fetch, parse, index, cleanup
    status: str  # pending, running, completed, failed
    details: Dict
    timestamp: str


class StateManager:
    def __init__(self, state_dir: str = "./cache", filename: str = "sync_checkpoints.jsonl"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.filepath = self.state_dir / filename

    def write_checkpoint(self, source_id: str, stage: str, status: str, details: Optional[Dict] = None) -> None:
        cp = Checkpoint(
            source_id=source_id,
            stage=stage,
            status=status,
            details=details or {},
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        with open(self.filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(cp), ensure_ascii=False) + "\n")

    def read_checkpoints(self, source_id: Optional[str] = None) -> List[Dict]:
        if not self.filepath.exists():
            return []
        rows: List[Dict] = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    if source_id is None or data.get("source_id") == source_id:
                        rows.append(data)
                except Exception:
                    logger.warning("Skipping malformed checkpoint line")
        return rows

    def latest_status(self, source_id: str) -> Optional[Dict]:
        cps = self.read_checkpoints(source_id)
        if not cps:
            return None
        # Already chronological by append order; last entry is latest
        return cps[-1]

