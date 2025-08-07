"""
External Monitoring Integration for the Sync Module.

This module provides a flexible system for sending sync operation reports
to external monitoring and alerting platforms. It is designed to be pluggable,
allowing for easy integration with services like Datadog, Slack, PagerDuty,
or any custom webhook endpoint.

Key Features:
- Webhook Integration: Sends detailed sync summaries as JSON payloads to any
  HTTP endpoint.
- Pluggable Architecture: Easily extendable to support other monitoring
  platforms.
- Actionable Payloads: The data sent is structured to be easily parsed and
  used for creating dashboards and alerts.
- Configurable: Endpoints and other settings are managed from the main
  sync_config.yaml.
"""

import requests
import json
from typing import Dict, Any, Optional
from .logging_manager import get_logger

logger = get_logger(__name__)

class ExternalMonitor:
    """
    Base class for external monitoring integrations.
    """
    def send_report(self, report: Dict[str, Any]):
        raise NotImplementedError

class WebhookMonitor(ExternalMonitor):
    """
    Sends sync reports to a custom webhook endpoint.
    """
    def __init__(self, endpoint_url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 10):
        self.endpoint_url = endpoint_url
        self.headers = headers or {'Content-Type': 'application/json'}
        self.timeout = timeout

    def send_report(self, report: Dict[str, Any]):
        """
        Sends the report as a JSON payload to the configured webhook.
        """
        try:
            response = requests.post(
                self.endpoint_url,
                data=json.dumps(report),
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            logger.info(f"Successfully sent report to webhook: {self.endpoint_url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send report to webhook {self.endpoint_url}: {e}")

def get_monitor_from_config(config) -> Optional[ExternalMonitor]:
    """
    Factory function to create a monitor instance from the sync configuration.
    """
    if not hasattr(config, 'monitoring') or not config.monitoring:
        return None
    
    monitor_config = config.monitoring
    monitor_type = monitor_config.get('type')

    if monitor_type == 'webhook':
        endpoint = monitor_config.get('endpoint_url')
        if endpoint:
            return WebhookMonitor(
                endpoint_url=endpoint,
                headers=monitor_config.get('headers'),
                timeout=monitor_config.get('timeout', 10)
            )
    
    logger.warning(f"Unknown or misconfigured monitor type: {monitor_type}")
    return None
