import yaml

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if "pipelines" not in cfg:
        raise ValueError("Missing top-level 'pipelines' key.")
    return cfg
