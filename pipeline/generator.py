from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from pipeline.settings import Settings, WhscMode

# Variação de emissão por motor — simula diferenças de calibração entre unidades.
# Valores são deltas aplicados sobre a emissão base calculada pelo modelo físico.
_ENGINE_BIAS: dict[str, dict[str, float]] = {
    "ENG-001": {"nox": +0.05, "pm": +0.002},   # NOx ligeiramente elevado
    "ENG-002": {"nox": -0.04, "pm": -0.001},   # motor mais eficiente
    "ENG-003": {"nox": +0.02, "pm": +0.004},   # PM acima da média
}

_CSV_COLUMNS = [
    "test_id", "engine_id", "test_cell", "timestamp",
    "whsc_mode", "mode_speed_pct", "mode_torque_pct", "mode_weight_factor",
    "sample_num", "rpm", "torque_nm", "power_kw", "fuel_flow_g_h",
    "boost_pressure_bar", "lambda",
    "nox_g_kwh", "co_g_kwh", "co2_g_kwh", "hc_g_kwh", "pm_g_kwh",
    "coolant_temp_c", "exhaust_temp_c", "oil_temp_c", "intake_air_temp_c",
]


class DynoDataGenerator:
    """Gera dados simulados de dinamômetro WHSC e salva em CSV.

    Não calcula flags de conformidade regulatória — essa lógica
    pertence à camada Silver (dbt), que lê os limites de config.yaml
    via vars do dbt_project.yml.
    """

    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._sim = settings.simulation
        self._rng = random.Random(self._sim.random_seed)

    def run(self) -> Path:
        records = self._generate_all_records()
        output = Path(self._s.local_csv_path)
        self._write_csv(records, output)
        print(f"  Gerados      : {len(records):,} registros")
        print(f"  Arquivo      : {output}")
        return output

    # ── geração de dados ───────────────────────────────────────────────────────

    def _generate_all_records(self) -> list[dict]:
        records: list[dict] = []
        base_time = datetime(2024, 1, 1)
        engines = list(self._sim.engines)
        cells = list(self._sim.test_cells)

        for run_num in range(1, self._sim.test_runs + 1):
            engine_id = engines[(run_num - 1) % len(engines)]
            test_cell = cells[(run_num - 1) % len(cells)]
            test_id = f"TEST-{run_num:03d}"
            run_start = base_time + timedelta(days=run_num - 1)
            records.extend(self._generate_test_run(test_id, engine_id, test_cell, run_start))

        return records

    def _generate_test_run(
        self,
        test_id: str,
        engine_id: str,
        test_cell: str,
        start_time: datetime,
    ) -> list[dict]:
        records: list[dict] = []
        current_time = start_time

        for mode_num, mode in self._s.whsc_modes.items():
            for sample_num in range(1, self._sim.samples_per_mode + 1):
                records.append(
                    self._generate_sample(
                        test_id, engine_id, test_cell,
                        mode_num, mode, sample_num, current_time,
                    )
                )
                current_time += timedelta(seconds=30)

        return records

    def _generate_sample(
        self,
        test_id: str,
        engine_id: str,
        test_cell: str,
        mode_num: int,
        mode: WhscMode,
        sample_num: int,
        timestamp: datetime,
    ) -> dict:
        rng = self._rng
        rated_rpm = self._sim.engine_rated_rpm
        max_torque = self._sim.engine_max_torque_nm
        bias = _ENGINE_BIAS.get(engine_id, {})
        load = mode.torque_pct / 100.0

        # Mecânica do motor
        rpm = max(mode.speed_pct / 100.0 * rated_rpm + rng.gauss(0, 10), 0.0)
        torque_nm = max(load * max_torque + rng.gauss(0, 15), 0.0)

        # P(kW) = T(Nm) × n(RPM) / 9549
        power_kw = torque_nm * rpm / 9549.0 if rpm > 0 else 0.0

        fuel_flow_g_h = max(power_kw * 220 + 500 + rng.gauss(0, 20), 300.0)

        # Lambda (razão ar-combustível) — diesel tipicamente > 1
        lam = max(1.5 + (1 - load) * 1.2 + rng.gauss(0, 0.05), 1.05)

        boost_bar = max(
            1.0 + (mode.speed_pct / 100.0) * 1.8 * load + rng.gauss(0, 0.02),
            1.0,
        )

        # Temperaturas (°C)
        exhaust_c = 250 + load * 350 + rng.gauss(0, 5)
        coolant_c = 85 + load * 10 + rng.gauss(0, 1)
        oil_c = 90 + load * 15 + rng.gauss(0, 1)
        intake_c = 25 + rng.gauss(0, 1)

        # Emissões (g/kWh) — sem flags de conformidade
        nox = max(0.15 + load * 0.45 + bias.get("nox", 0) + rng.gauss(0, 0.03), 0.05)
        co = max(2.5 * (1 - load) + 0.3 + rng.gauss(0, 0.10), 0.10)
        co2 = 450 + load * 150 + rng.gauss(0, 10)
        hc = max(0.08 * (1 - load) + 0.04 + rng.gauss(0, 0.005), 0.01)
        pm = max(0.004 + load * 0.018 + bias.get("pm", 0) + rng.gauss(0, 0.001), 0.001)

        return {
            "test_id":            test_id,
            "engine_id":          engine_id,
            "test_cell":          test_cell,
            "timestamp":          timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "whsc_mode":          mode_num,
            "mode_speed_pct":     round(mode.speed_pct, 2),
            "mode_torque_pct":    round(mode.torque_pct, 2),
            "mode_weight_factor": round(mode.weight_factor, 4),
            "sample_num":         sample_num,
            "rpm":                round(rpm, 1),
            "torque_nm":          round(torque_nm, 1),
            "power_kw":           round(power_kw, 2),
            "fuel_flow_g_h":      round(fuel_flow_g_h, 1),
            "boost_pressure_bar": round(boost_bar, 3),
            "lambda":             round(lam, 3),
            "nox_g_kwh":          round(nox, 4),
            "co_g_kwh":           round(co, 4),
            "co2_g_kwh":          round(co2, 2),
            "hc_g_kwh":           round(hc, 4),
            "pm_g_kwh":           round(pm, 5),
            "coolant_temp_c":     round(coolant_c, 1),
            "exhaust_temp_c":     round(exhaust_c, 1),
            "oil_temp_c":         round(oil_c, 1),
            "intake_air_temp_c":  round(intake_c, 1),
        }

    # ── I/O ───────────────────────────────────────────────────────────────────

    @staticmethod
    def _write_csv(records: list[dict], path: Path) -> None:
        if not records:
            raise ValueError("Nenhum registro para gravar.")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(records)
