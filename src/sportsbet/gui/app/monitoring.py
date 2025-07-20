"""Monitoring and observability system for the sports betting application."""

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum
import threading

logger = logging.getLogger(__name__)

# Monitoring configuration
MONITORING_CONFIG = {
    'metrics': {
        'collection_interval': 30,  # seconds
        'retention_period': 3600,  # 1 hour
        'max_data_points': 1000
    },
    'health_checks': {
        'interval': 60,  # seconds
        'timeout': 10,  # seconds
        'failure_threshold': 3
    },
    'alerts': {
        'cooldown_period': 300,  # 5 minutes
        'max_alerts_per_hour': 10
    }
}


class MetricType(Enum):
    """Types of metrics that can be collected."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class MetricPoint:
    """Represents a single metric data point."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: datetime
    tags: Dict[str, str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'value': self.value,
            'type': self.metric_type.value,
            'timestamp': self.timestamp.isoformat(),
            'tags': self.tags or {}
        }


@dataclass
class HealthCheck:
    """Represents a health check configuration."""
    name: str
    check_function: Callable
    interval: int
    timeout: int
    failure_threshold: int
    is_critical: bool = True
    
    def __post_init__(self):
        self.failure_count = 0
        self.last_check = None
        self.last_success = None
        self.is_healthy = True


@dataclass
class Alert:
    """Represents an alert/notification."""
    id: str
    title: str
    message: str
    severity: AlertSeverity
    timestamp: datetime
    source: str
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'title': self.title,
            'message': self.message,
            'severity': self.severity.value,
            'timestamp': self.timestamp.isoformat(),
            'source': self.source,
            'resolved': self.resolved,
            'resolved_at': self.resolved_at.isoformat() if self.resolved_at else None
        }


class MetricsCollector:
    """Collects and stores application metrics."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or MONITORING_CONFIG['metrics']
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.config['max_data_points']))
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = defaultdict(float)
        self._lock = threading.RLock()
        self.is_collecting = False
    
    def start_collection(self):
        """Start metrics collection."""
        if not self.is_collecting:
            self.is_collecting = True
            threading.Thread(target=self._collection_loop, daemon=True).start()
            logger.info("Metrics collection started")
    
    def stop_collection(self):
        """Stop metrics collection."""
        self.is_collecting = False
        logger.info("Metrics collection stopped")
    
    def _collection_loop(self):
        """Main collection loop."""
        while self.is_collecting:
            try:
                self._collect_system_metrics()
                time.sleep(self.config['collection_interval'])
            except Exception as e:
                logger.error(f"Error in metrics collection: {e}")
                time.sleep(self.config['collection_interval'])
    
    def _collect_system_metrics(self):
        """Collect system-level metrics."""
        try:
            import psutil
            
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            self.record_gauge('system.cpu.usage_percent', cpu_percent)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            self.record_gauge('system.memory.usage_percent', memory.percent)
            self.record_gauge('system.memory.available_mb', memory.available / 1024 / 1024)
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            self.record_gauge('system.disk.usage_percent', (disk.used / disk.total) * 100)
            self.record_gauge('system.disk.free_gb', disk.free / 1024 / 1024 / 1024)
            
        except ImportError:
            # psutil not available, skip system metrics
            pass
        except Exception as e:
            logger.warning(f"Failed to collect system metrics: {e}")
    
    def record_counter(self, name: str, value: float = 1.0, tags: Dict[str, str] = None):
        """Record a counter metric."""
        with self._lock:
            self.counters[name] += value
            
            metric_point = MetricPoint(
                name=name,
                value=self.counters[name],
                metric_type=MetricType.COUNTER,
                timestamp=datetime.now(),
                tags=tags
            )
            
            self.metrics[name].append(metric_point)
    
    def record_gauge(self, name: str, value: float, tags: Dict[str, str] = None):
        """Record a gauge metric."""
        with self._lock:
            self.gauges[name] = value
            
            metric_point = MetricPoint(
                name=name,
                value=value,
                metric_type=MetricType.GAUGE,
                timestamp=datetime.now(),
                tags=tags
            )
            
            self.metrics[name].append(metric_point)
    
    def record_timer(self, name: str, duration: float, tags: Dict[str, str] = None):
        """Record a timer metric."""
        with self._lock:
            metric_point = MetricPoint(
                name=name,
                value=duration,
                metric_type=MetricType.TIMER,
                timestamp=datetime.now(),
                tags=tags
            )
            
            self.metrics[name].append(metric_point)
    
    def get_metrics(self, name: str = None, since: datetime = None) -> List[MetricPoint]:
        """Get metrics data."""
        with self._lock:
            if name:
                metrics_data = list(self.metrics.get(name, []))
            else:
                metrics_data = []
                for metric_name, points in self.metrics.items():
                    metrics_data.extend(points)
            
            if since:
                metrics_data = [point for point in metrics_data if point.timestamp >= since]
            
            return sorted(metrics_data, key=lambda x: x.timestamp)
    
    def get_metric_names(self) -> List[str]:
        """Get list of metric names."""
        with self._lock:
            return list(self.metrics.keys())
    
    def get_latest_value(self, name: str) -> Optional[float]:
        """Get the latest value for a metric."""
        with self._lock:
            if name in self.metrics and self.metrics[name]:
                return self.metrics[name][-1].value
            return None
    
    def cleanup_old_data(self):
        """Clean up old metric data."""
        cutoff_time = datetime.now() - timedelta(seconds=self.config['retention_period'])
        
        with self._lock:
            for name, points in self.metrics.items():
                # Remove old data points
                while points and points[0].timestamp < cutoff_time:
                    points.popleft()


class HealthChecker:
    """Manages application health checks."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or MONITORING_CONFIG['health_checks']
        self.health_checks: Dict[str, HealthCheck] = {}
        self.is_running = False
        self._lock = threading.RLock()
    
    def register_health_check(
        self, 
        name: str, 
        check_function: Callable, 
        interval: int = None,
        timeout: int = None,
        failure_threshold: int = None,
        is_critical: bool = True
    ):
        """Register a new health check."""
        health_check = HealthCheck(
            name=name,
            check_function=check_function,
            interval=interval or self.config['interval'],
            timeout=timeout or self.config['timeout'],
            failure_threshold=failure_threshold or self.config['failure_threshold'],
            is_critical=is_critical
        )
        
        with self._lock:
            self.health_checks[name] = health_check
        
        logger.info(f"Registered health check: {name}")
    
    def start_monitoring(self):
        """Start health check monitoring."""
        if not self.is_running:
            self.is_running = True
            threading.Thread(target=self._monitoring_loop, daemon=True).start()
            logger.info("Health check monitoring started")
    
    def stop_monitoring(self):
        """Stop health check monitoring."""
        self.is_running = False
        logger.info("Health check monitoring stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.is_running:
            try:
                self._run_health_checks()
                time.sleep(10)  # Check every 10 seconds
            except Exception as e:
                logger.error(f"Error in health check monitoring: {e}")
                time.sleep(10)
    
    def _run_health_checks(self):
        """Run all due health checks."""
        current_time = datetime.now()
        
        for name, health_check in list(self.health_checks.items()):
            try:
                # Check if it's time to run this health check
                if (health_check.last_check is None or 
                    (current_time - health_check.last_check).total_seconds() >= health_check.interval):
                    
                    self._execute_health_check(name, health_check)
                    
            except Exception as e:
                logger.error(f"Error running health check {name}: {e}")
    
    def _execute_health_check(self, name: str, health_check: HealthCheck):
        """Execute a single health check."""
        try:
            health_check.last_check = datetime.now()
            
            # Run the health check with timeout
            result = self._run_with_timeout(health_check.check_function, health_check.timeout)
            
            if result:
                # Health check passed
                health_check.failure_count = 0
                health_check.last_success = datetime.now()
                if not health_check.is_healthy:
                    health_check.is_healthy = True
                    logger.info(f"Health check {name} recovered")
            else:
                # Health check failed
                health_check.failure_count += 1
                logger.warning(f"Health check {name} failed (attempt {health_check.failure_count})")
                
                if health_check.failure_count >= health_check.failure_threshold:
                    if health_check.is_healthy:
                        health_check.is_healthy = False
                        logger.error(f"Health check {name} marked as unhealthy")
        
        except Exception as e:
            health_check.failure_count += 1
            logger.error(f"Health check {name} failed with exception: {e}")
    
    def _run_with_timeout(self, func: Callable, timeout: int) -> bool:
        """Run a function with timeout."""
        import threading
        import queue
        
        result_queue = queue.Queue()
        
        def target():
            try:
                result = func()
                result_queue.put(('success', result))
            except Exception as e:
                result_queue.put(('error', e))
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout)
        
        if thread.is_alive():
            # Timeout occurred
            return False
        
        try:
            status, result = result_queue.get_nowait()
            return status == 'success' and result
        except queue.Empty:
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status."""
        with self._lock:
            all_healthy = True
            critical_issues = []
            health_details = {}
            
            for name, health_check in self.health_checks.items():
                is_healthy = health_check.is_healthy
                health_details[name] = {
                    'healthy': is_healthy,
                    'last_check': health_check.last_check.isoformat() if health_check.last_check else None,
                    'last_success': health_check.last_success.isoformat() if health_check.last_success else None,
                    'failure_count': health_check.failure_count,
                    'is_critical': health_check.is_critical
                }
                
                if not is_healthy:
                    all_healthy = False
                    if health_check.is_critical:
                        critical_issues.append(name)
            
            return {
                'overall_healthy': all_healthy,
                'critical_issues': critical_issues,
                'checks': health_details,
                'timestamp': datetime.now().isoformat()
            }


class AlertManager:
    """Manages alerts and notifications."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or MONITORING_CONFIG['alerts']
        self.alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.alert_cooldowns: Dict[str, datetime] = {}
        self._lock = threading.RLock()
        self.subscribers: Set[Callable] = set()
    
    def subscribe(self, callback: Callable[[Alert], None]):
        """Subscribe to alert notifications."""
        self.subscribers.add(callback)
    
    def unsubscribe(self, callback: Callable[[Alert], None]):
        """Unsubscribe from alert notifications."""
        self.subscribers.discard(callback)
    
    def create_alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        source: str,
        alert_id: str = None
    ) -> str:
        """Create a new alert."""
        import uuid
        
        if not alert_id:
            alert_id = str(uuid.uuid4())
        
        # Check cooldown
        cooldown_key = f"{source}:{title}"
        if cooldown_key in self.alert_cooldowns:
            last_alert = self.alert_cooldowns[cooldown_key]
            if (datetime.now() - last_alert).total_seconds() < self.config['cooldown_period']:
                logger.debug(f"Alert {title} is in cooldown period")
                return alert_id
        
        alert = Alert(
            id=alert_id,
            title=title,
            message=message,
            severity=severity,
            timestamp=datetime.now(),
            source=source
        )
        
        with self._lock:
            self.alerts[alert_id] = alert
            self.alert_history.append(alert)
            self.alert_cooldowns[cooldown_key] = datetime.now()
        
        # Notify subscribers
        self._notify_subscribers(alert)
        
        logger.info(f"Created {severity.value} alert: {title}")
        return alert_id
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert."""
        with self._lock:
            if alert_id in self.alerts:
                alert = self.alerts[alert_id]
                alert.resolved = True
                alert.resolved_at = datetime.now()
                del self.alerts[alert_id]
                
                logger.info(f"Resolved alert: {alert.title}")
                return True
        
        return False
    
    def get_active_alerts(self, severity: AlertSeverity = None) -> List[Alert]:
        """Get active alerts."""
        with self._lock:
            alerts = list(self.alerts.values())
            
            if severity:
                alerts = [alert for alert in alerts if alert.severity == severity]
            
            return sorted(alerts, key=lambda x: x.timestamp, reverse=True)
    
    def get_alert_history(self, limit: int = 100) -> List[Alert]:
        """Get alert history."""
        with self._lock:
            return sorted(self.alert_history[-limit:], key=lambda x: x.timestamp, reverse=True)
    
    def _notify_subscribers(self, alert: Alert):
        """Notify alert subscribers."""
        for callback in self.subscribers:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Error notifying alert subscriber: {e}")


class MonitoringSystem:
    """Central monitoring system that coordinates all monitoring components."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or MONITORING_CONFIG
        self.metrics_collector = MetricsCollector(self.config.get('metrics'))
        self.health_checker = HealthChecker(self.config.get('health_checks'))
        self.alert_manager = AlertManager(self.config.get('alerts'))
        self.is_running = False
        
        # Register default health checks
        self._register_default_health_checks()
        
        # Subscribe to health check failures
        self._setup_alert_integration()
    
    def start(self):
        """Start the monitoring system."""
        if not self.is_running:
            self.is_running = True
            self.metrics_collector.start_collection()
            self.health_checker.start_monitoring()
            
            logger.info("Monitoring system started")
    
    def stop(self):
        """Stop the monitoring system."""
        if self.is_running:
            self.is_running = False
            self.metrics_collector.stop_collection()
            self.health_checker.stop_monitoring()
            
            logger.info("Monitoring system stopped")
    
    def _register_default_health_checks(self):
        """Register default health checks."""
        # System health check
        def system_health_check():
            try:
                import psutil
                cpu_percent = psutil.cpu_percent(interval=1)
                memory_percent = psutil.virtual_memory().percent
                
                # System is healthy if CPU < 90% and Memory < 90%
                return cpu_percent < 90 and memory_percent < 90
            except ImportError:
                return True  # Assume healthy if psutil not available
        
        self.health_checker.register_health_check(
            'system_resources',
            system_health_check,
            interval=60,
            is_critical=True
        )
    
    def _setup_alert_integration(self):
        """Setup integration between components for alerting."""
        def health_check_alert_callback():
            health_status = self.health_checker.get_health_status()
            
            if not health_status['overall_healthy']:
                for issue in health_status['critical_issues']:
                    self.alert_manager.create_alert(
                        title=f"Health Check Failed: {issue}",
                        message=f"Critical health check {issue} is failing",
                        severity=AlertSeverity.CRITICAL,
                        source="health_checker"
                    )
        
        # Check for health issues periodically
        def monitoring_loop():
            while self.is_running:
                try:
                    health_check_alert_callback()
                    time.sleep(60)  # Check every minute
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {e}")
                    time.sleep(60)
        
        threading.Thread(target=monitoring_loop, daemon=True).start()
    
    def record_metric(self, metric_type: MetricType, name: str, value: float, tags: Dict[str, str] = None):
        """Record a metric."""
        if metric_type == MetricType.COUNTER:
            self.metrics_collector.record_counter(name, value, tags)
        elif metric_type == MetricType.GAUGE:
            self.metrics_collector.record_gauge(name, value, tags)
        elif metric_type == MetricType.TIMER:
            self.metrics_collector.record_timer(name, value, tags)
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for monitoring dashboard."""
        health_status = self.health_checker.get_health_status()
        active_alerts = self.alert_manager.get_active_alerts()
        
        # Get key metrics
        key_metrics = {}
        for metric_name in ['system.cpu.usage_percent', 'system.memory.usage_percent', 'system.disk.usage_percent']:
            latest_value = self.metrics_collector.get_latest_value(metric_name)
            if latest_value is not None:
                key_metrics[metric_name] = latest_value
        
        return {
            'health_status': health_status,
            'active_alerts': [alert.to_dict() for alert in active_alerts],
            'key_metrics': key_metrics,
            'metric_names': self.metrics_collector.get_metric_names(),
            'timestamp': datetime.now().isoformat()
        }


# Global monitoring system
monitoring_system = MonitoringSystem()


def start_monitoring():
    """Start the global monitoring system."""
    monitoring_system.start()


def stop_monitoring():
    """Stop the global monitoring system."""
    monitoring_system.stop()


# Convenience functions for recording metrics
def record_counter(name: str, value: float = 1.0, tags: Dict[str, str] = None):
    """Record a counter metric."""
    monitoring_system.record_metric(MetricType.COUNTER, name, value, tags)


def record_gauge(name: str, value: float, tags: Dict[str, str] = None):
    """Record a gauge metric."""
    monitoring_system.record_metric(MetricType.GAUGE, name, value, tags)


def record_timer(name: str, duration: float, tags: Dict[str, str] = None):
    """Record a timer metric."""
    monitoring_system.record_metric(MetricType.TIMER, name, duration, tags)


# Context manager for timing operations
class timer:
    """Context manager for timing operations."""
    
    def __init__(self, name: str, tags: Dict[str, str] = None):
        self.name = name
        self.tags = tags
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            record_timer(self.name, duration, self.tags)