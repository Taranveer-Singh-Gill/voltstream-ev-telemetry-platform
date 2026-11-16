
import random
from datetime import datetime, timezone


def generate_telemetry(vehicle_id: str) -> dict:
    """Generate a simulated EV telemetry event."""

    return {
        "vehicle_id": vehicle_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "battery_soc": round(random.uniform(15, 100), 2),
        "battery_temp_c": round(random.uniform(20, 55), 2),
        "battery_voltage_v": round(random.uniform(300, 420), 2),
        "speed_kmh": round(random.uniform(0, 120), 2),
    }


if __name__ == "__main__":
    for vehicle_number in range(1001, 1006):
        event = generate_telemetry(f"EV-{vehicle_number}")
        print(event)
