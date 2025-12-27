import yaml

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if "sources" not in cfg or "targets" not in cfg:
        raise ValueError("Missing 'sources' or 'targets' in config.")
    return cfg
