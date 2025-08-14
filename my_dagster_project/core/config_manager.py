import os
from typing import Dict, Any
import yaml

class ConfigManager:
    """Centralized configuration management for different environments"""
    
    def __init__(self, config_path: str = "config/app_config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            # Return default configuration
            return self._get_default_config()
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values"""
        return {
            "environment": "development",
            "database": {
                "metadata_db": "metadata.db",
                "monitoring_db": "monitoring.db"
            },
            "monitoring": {
                "enable_alerts": True,
                "enable_sla_tracking": True,
                "alert_thresholds": {
                    "data_volume_drop": 0.2,  # 20% drop threshold
                    "execution_time_increase": 0.5  # 50% increase threshold
                }
            },
            "governance": {
                "enable_auto_discovery": True,
                "auto_discovery_interval": 3600,  # 1 hour in seconds
                "require_asset_ownership": True
            }
        }
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value using dot notation (e.g., 'database.metadata_db')"""
        keys = key_path.split('.')
        value = self.config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_environment(self) -> str:
        """Get current environment"""
        return self.get("environment", "development")
    
    def is_production(self) -> bool:
        """Check if current environment is production"""
        return self.get_environment().lower() == "production"

# Global instance
config_manager = ConfigManager()