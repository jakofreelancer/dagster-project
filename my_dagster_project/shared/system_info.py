import os
import platform
import getpass
import socket
from typing import Dict, Any

def get_system_info() -> Dict[str, Any]:
    """Get system information for asset registration"""
    try:
        # Try to get container ID if running in Docker
        container_id = "unknown"
        try:
            # Check if we're in a Docker container
            if os.path.exists("/.dockerenv"):
                # Try to get container ID from cgroup
                with open("/proc/self/cgroup", "r") as f:
                    for line in f:
                        if "docker" in line:
                            container_id = line.strip().split("/")[-1][:12]
                            break
        except:
            pass
        
        return {
            "server_name": os.environ.get("HOSTNAME", platform.node()),
            "host_name": socket.gethostname(),
            "machine_name": platform.machine(),
            "logged_user_name": getpass.getuser(),
            "operating_system": platform.system(),
            "os_version": platform.version(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
            "environment": os.environ.get("ENVIRONMENT", "development"),
            "project_name": os.environ.get("PROJECT_NAME", "dagster-project"),
            "container_id": container_id,
            "is_containerized": os.path.exists("/.dockerenv")
        }
    except Exception as e:
        # Fallback in case of any errors
        return {
            "server_name": os.environ.get("HOSTNAME", "unknown"),
            "host_name": "unknown",
            "machine_name": "unknown",
            "logged_user_name": "unknown",
            "operating_system": "unknown",
            "os_version": "unknown",
            "processor": "unknown",
            "python_version": "unknown",
            "environment": os.environ.get("ENVIRONMENT", "development"),
            "project_name": os.environ.get("PROJECT_NAME", "dagster-project"),
            "container_id": "unknown",
            "is_containerized": os.path.exists("/.dockerenv")
        }

def get_environment_info() -> str:
    """Get the current environment (development, staging, production, etc.)"""
    # Check environment variables in order of preference
    env = (os.environ.get("ENVIRONMENT") or 
           os.environ.get("ENV") or 
           os.environ.get("DEPLOYMENT_ENV") or 
           "development")
    return env.lower()

def get_project_name() -> str:
    """Get the project name"""
    return (os.environ.get("PROJECT_NAME") or 
            os.environ.get("APP_NAME") or 
            os.environ.get("SERVICE_NAME") or 
            "dagster-project")