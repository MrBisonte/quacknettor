import os
import requests
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Base URL for Jules API (Placeholder based on standard Google APIs, to be confirmed)
JULES_API_BASE = "https://jules.googleapis.com/v1alpha" 

class JulesClient:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("JULES_API_KEY")
        if not self.api_key:
            logger.warning("Jules API Key not found. AI features will be disabled.")
    
    def is_configured(self) -> bool:
        return bool(self.api_key)

    def create_session(self, prompt: str, source_uri: str = "github.com/MrBisonte/quacknettor") -> Dict[str, Any]:
        """
        Creates a coding session with Jules.
        """
        if not self.api_key:
            return {"error": "API Key missing"}

        # MOCK RESPONSE for Demonstration
        logger.info(f"Mocking Jules Session creation for prompt: {prompt}")
        return {
            "name": "projects/quacknettor/sessions/12345",
            "state": "ACTIVE",
            "activities": [
                {"type": "PLANNING", "status": "IN_PROGRESS", "description": "Analyzing repository via DuckDB connection..."}
            ]
        }

    def get_session(self, session_name: str) -> Dict[str, Any]:
        """
        Retrieves the latest state of a session.
        """
        if not self.api_key:
            return {"error": "API Key missing"}
            
        # Mock Response
        return {
            "name": session_name,
            "state": "COMPLETED",
            "activities": [
                {"type": "PLANNING", "status": "DONE", "description": "Plan generated."},
                {"type": "CODING", "status": "DONE", "description": "Generated pipeline configuration for S3 source."}
            ],
            "artifacts": [
                {"name": "pipelines.yml (suggested)", "content_snippet": "sources:\n  s3_new:\n    type: parquet..."}
            ]
        }
