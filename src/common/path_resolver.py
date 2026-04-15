# src/common/path_resolver.py
import os

class PathResolver:
    @staticmethod
    def resolve(base_path: str, relative_path: str, run_mode: str) -> str:
        """
        Handles the discrepancy between local absolute paths 
        and cloud protocol-based paths.
        """
        # Clean the relative path of leading slashes
        clean_rel = relative_path.lstrip("/")
        
        if run_mode == "local":
            # Uses OS-specific separators (\ for Windows, / for Linux)
            return os.path.abspath(os.path.join(base_path, clean_rel))
        
        # For Cloud (Azure/Databricks), treat as a URI
        # Ensures we don't accidentally double-slash
        return f"{base_path.rstrip('/')}/{clean_rel}"