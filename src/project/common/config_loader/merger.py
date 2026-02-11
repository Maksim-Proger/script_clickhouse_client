from typing import Dict, Any

# def merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
#     result = base.copy()
#     for k, v in override.items():
#         if k in result and isinstance(result[k], dict) and isinstance(v, dict):
#             result[k] = merge_dicts(result[k], v)
#         else:
#             result[k] = v
#     return result
