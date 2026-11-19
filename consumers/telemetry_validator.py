
from datetime import datetime


REQUIRED_FIELDS = {
    "vehicle_id",
    "timestamp",
    "battery_soc",
    "battery_temp_c",
    "battery_voltage_v",
    "speed_kmh",
}

NUMERIC_RANGES = {
    "battery_soc": (0, 100),
    "battery_temp_c": (-40, 100),
    "battery_voltage_v": (0, 1000),
    "speed_kmh": (0, 250),
}


def validate_telemetry(event: dict) -> tuple[bool, str]:
    if not isinstance(event, dict):
        return False, "Telemetry must be a JSON object"

    missing = REQUIRED_FIELDS - event.keys()

    if missing:
        return False, f"Missing fields: {sorted(missing)}"

    vehicle_id = event["vehicle_id"]

    if not isinstance(vehicle_id, str) or not vehicle_id.strip():
        return False, "Invalid vehicle_id"

    try:
        timestamp = datetime.fromisoformat(event["timestamp"])

        if timestamp.tzinfo is None:
            return False, "Timestamp must include timezone"

    except (ValueError, TypeError):
        return False, "Invalid timestamp"

    for field, (minimum, maximum) in NUMERIC_RANGES.items():
        value = event[field]

        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not minimum <= value <= maximum
        ):
            return False, f"Invalid {field}: {value}"

    return True, "Valid telemetry"
