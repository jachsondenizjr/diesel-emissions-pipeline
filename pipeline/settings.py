from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

# Resolve config.yaml a partir da raiz do projeto (um nível acima de pipeline/)
_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


# ── Dataclasses por seção ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class GcpSettings:
    project_id: str
    region: str
    credentials_path: str


@dataclass(frozen=True)
class GcsSettings:
    bucket: str
    raw_prefix: str


@dataclass(frozen=True)
class BigQuerySettings:
    dataset_raw: str
    table_raw: str


@dataclass(frozen=True)
class EmissionLimits:
    nox: float
    co: float
    hc: float
    pm: float


@dataclass(frozen=True)
class RegulatoryLimits:
    epa_tier4: EmissionLimits
    euro6: EmissionLimits


@dataclass(frozen=True)
class WhscMode:
    speed_pct: float
    torque_pct: float
    weight_factor: float


@dataclass(frozen=True)
class SimulationSettings:
    test_runs: int
    samples_per_mode: int
    random_seed: int
    engines: tuple[str, ...]
    test_cells: tuple[str, ...]
    engine_rated_rpm: float
    engine_max_torque_nm: float


# ── Settings principal ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Settings:
    gcp: GcpSettings
    gcs: GcsSettings
    bigquery: BigQuerySettings
    local_csv_path: str
    regulatory_limits: RegulatoryLimits
    whsc_modes: dict[int, WhscMode]
    simulation: SimulationSettings

    @property
    def bq_full_table(self) -> str:
        return f"{self.gcp.project_id}.{self.bigquery.dataset_raw}.{self.bigquery.table_raw}"

    def validate(self) -> None:
        creds = Path(self.gcp.credentials_path)
        if not creds.exists():
            raise FileNotFoundError(
                f"Credencial não encontrada: {self.gcp.credentials_path}\n"
                "Crie uma Service Account no GCP com os papéis:\n"
                "  - Storage Object Admin\n"
                "  - BigQuery Data Editor\n"
                "  - BigQuery Job User"
            )


# ── Loader ─────────────────────────────────────────────────────────────────────

def load_settings(config_path: Path = _CONFIG_PATH) -> Settings:
    with open(config_path, encoding="utf-8") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh)

    limits = raw["regulatory_limits"]
    whsc_modes = {
        int(mode_num): WhscMode(**mode_data)
        for mode_num, mode_data in raw["whsc_modes"].items()
    }

    sim = raw["simulation"]

    return Settings(
        gcp=GcpSettings(**raw["gcp"]),
        gcs=GcsSettings(**raw["gcs"]),
        bigquery=BigQuerySettings(**raw["bigquery"]),
        local_csv_path=raw["local"]["csv_path"],
        regulatory_limits=RegulatoryLimits(
            epa_tier4=EmissionLimits(**limits["epa_tier4"]),
            euro6=EmissionLimits(**limits["euro6"]),
        ),
        whsc_modes=whsc_modes,
        simulation=SimulationSettings(
            test_runs=sim["test_runs"],
            samples_per_mode=sim["samples_per_mode"],
            random_seed=sim["random_seed"],
            engines=tuple(sim["engines"]),
            test_cells=tuple(sim["test_cells"]),
            engine_rated_rpm=sim["engine_rated_rpm"],
            engine_max_torque_nm=sim["engine_max_torque_nm"],
        ),
    )
