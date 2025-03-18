def sanitize_json(obj):
    """Replace NaN values with null in JSON data."""
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_json(x) for x in obj]
    elif isinstance(obj, float) and (str(obj) == "nan" or str(obj) == "NaN"):
        return None
    return obj
