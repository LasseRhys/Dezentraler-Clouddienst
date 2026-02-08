from flask import Flask, request, jsonify
import redis
import json
import time
import threading
from datetime import datetime, timedelta
from prometheus_client import Counter, Gauge, Histogram, Summary, Info, generate_latest, REGISTRY
import logging
import os
import random
from collections import defaultdict

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('middleman.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Redis Connection
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    decode_responses=True
)

# ===== PROMETHEUS METRIKEN =====

# Basis-Zähler
host_registrations = Counter('host_registrations_total', 'Total host registrations')
task_submissions = Counter('task_submissions_total', 'Total task submissions', ['task_type'])
task_completions = Counter('task_completions_total', 'Total task completions', ['status'])
task_assignments = Counter('task_assignments_total', 'Total task assignments')
shard_creations = Counter('task_shards_created_total', 'Total task shards created')
shard_completions = Counter('shard_completions_total', 'Total shard completions', ['status'])

# Gauges für aktuelle Zustände
active_hosts = Gauge('active_hosts', 'Number of active hosts', ['location'])
pending_tasks = Gauge('pending_tasks', 'Number of pending tasks')
pending_shards = Gauge('pending_shards', 'Number of pending shards')
running_shards = Gauge('running_shards', 'Number of running shards')
active_shards_per_host = Gauge('active_shards_per_host', 'Active shards per host', ['host_id', 'location'])

# Ressourcen-Metriken
resource_capacity = Gauge('resource_capacity', 'Total resource capacity', ['resource_type', 'location'])
resource_used = Gauge('resource_used', 'Used resources', ['resource_type', 'location'])
resource_available = Gauge('resource_available', 'Available resources', ['resource_type', 'location'])
resource_utilization = Gauge('resource_utilization_percent', 'Resource utilization percentage',
                             ['resource_type', 'location'])

# Host-spezifische Metriken
host_cpu_capacity = Gauge('host_cpu_capacity', 'CPU capacity per host', ['host_id', 'location'])
host_cpu_used = Gauge('host_cpu_used', 'CPU used per host', ['host_id', 'location'])
host_memory_capacity = Gauge('host_memory_capacity', 'Memory capacity per host', ['host_id', 'location'])
host_memory_used = Gauge('host_memory_used', 'Memory used per host', ['host_id', 'location'])
host_tasks_completed = Gauge('host_tasks_completed', 'Tasks completed per host', ['host_id', 'location'])
host_tasks_failed = Gauge('host_tasks_failed', 'Tasks failed per host', ['host_id', 'location'])
host_uptime = Gauge('host_uptime_seconds', 'Host uptime in seconds', ['host_id', 'location'])

# Performance-Metriken
task_completion_time = Histogram(
    'task_completion_seconds',
    'Task completion time',
    ['task_type', 'sharded'],
    buckets=[5, 10, 30, 60, 120, 300, 600, 1800]
)
shard_completion_time = Histogram(
    'shard_completion_seconds',
    'Shard completion time',
    ['location'],
    buckets=[1, 5, 10, 30, 60, 120, 300]
)
scheduling_latency = Histogram(
    'scheduling_latency_seconds',
    'Time from shard creation to assignment',
    buckets=[0.1, 0.5, 1, 5, 10, 30, 60]
)
task_queue_wait_time = Histogram(
    'task_queue_wait_seconds',
    'Time tasks spend in queue',
    buckets=[1, 5, 10, 30, 60, 120, 300]
)

# Netzwerk-Metriken
network_latency = Gauge('network_latency_ms', 'Network latency between hosts', ['from_host', 'to_host'])
cross_region_assignments = Counter('cross_region_assignments_total', 'Assignments across regions',
                                   ['from_region', 'to_region'])

# Effizienz-Metriken
bin_packing_efficiency = Gauge('bin_packing_efficiency', 'Bin packing efficiency score')
shard_distribution_variance = Gauge('shard_distribution_variance', 'Variance in shard distribution across hosts')
resource_fragmentation = Gauge('resource_fragmentation', 'Resource fragmentation index', ['resource_type'])

# Fehler-Metriken
scheduling_failures = Counter('scheduling_failures_total', 'Total scheduling failures', ['reason'])
host_failures = Counter('host_failures_total', 'Total host failures', ['host_id', 'location'])
shard_reassignments = Counter('shard_reassignments_total', 'Total shard reassignments', ['reason'])

# System-Info
system_info = Info('middleman_info', 'Middleman system information')
system_info.info({
    'version': '1.0.0',
    'start_time': str(datetime.now()),
    'scheduling_algorithm': 'bin_packing'
})


class MetricsCollector:
    """Sammelt und berechnet erweiterte Metriken"""

    def __init__(self):
        self.task_start_times = {}
        self.shard_start_times = {}
        self.location_stats = defaultdict(lambda: {'tasks': 0, 'shards': 0, 'failures': 0})

    def record_task_submitted(self, task_id, task_type, shardable):
        self.task_start_times[task_id] = time.time()
        task_submissions.labels(task_type=task_type).inc()

    def record_task_completed(self, task_id, task_type, sharded, status):
        if task_id in self.task_start_times:
            duration = time.time() - self.task_start_times[task_id]
            task_completion_time.labels(
                task_type=task_type,
                sharded='yes' if sharded else 'no'
            ).observe(duration)
            del self.task_start_times[task_id]

        task_completions.labels(status=status).inc()

    def record_shard_assigned(self, shard_id, created_at):
        latency = time.time() - created_at
        scheduling_latency.observe(latency)
        self.shard_start_times[shard_id] = time.time()

    def record_shard_completed(self, shard_id, location, success):
        if shard_id in self.shard_start_times:
            duration = time.time() - self.shard_start_times[shard_id]
            shard_completion_time.labels(location=location).observe(duration)
            del self.shard_start_times[shard_id]

        status = 'success' if success else 'failed'
        shard_completions.labels(status=status).inc()


metrics_collector = MetricsCollector()


class ResourceMiddleman:
    def __init__(self):
        self.hosts = {}
        self.tasks = {}
        self.shards = {}
        self.task_counter = 0
        self.shard_counter = 0
        self.heartbeat_timeout = 30
        self.ping_matrix = {}

    def register_host(self, host_data):
        """Registriert einen neuen Host"""
        host_id = host_data.get('host_id')

        host_info = {
            'host_id': host_id,
            'name': host_data.get('name'),
            'location': host_data.get('location', 'unknown'),
            'cpu_capacity': host_data.get('cpu_capacity', 1),
            'memory_capacity': host_data.get('memory_capacity', 1024),
            'cpu_available': host_data.get('cpu_capacity', 1),
            'memory_available': host_data.get('memory_capacity', 1024),
            'status': 'online',
            'last_heartbeat': time.time(),
            'registered_at': time.time(),
            'tasks_completed': 0,
            'tasks_failed': 0,
            'endpoint': host_data.get('endpoint')
        }

        self.hosts[host_id] = host_info
        redis_client.set(f'host:{host_id}', json.dumps(host_info))

        self._initialize_ping_for_host(host_id)

        # Metriken aktualisieren
        host_registrations.inc()
        self._update_host_metrics(host_id)
        self._update_location_metrics()

        logger.info(f"Host registriert: {host_id} ({host_info['name']}) in {host_info['location']}")
        return True

    def _initialize_ping_for_host(self, host_id):
        """Initialisiert simulierte Ping-Zeiten für einen Host"""
        if host_id not in self.ping_matrix:
            self.ping_matrix[host_id] = {}

        for other_host_id in self.hosts.keys():
            if other_host_id != host_id:
                if self.hosts[host_id]['location'] == self.hosts[other_host_id]['location']:
                    ping = random.uniform(5, 15)
                else:
                    ping = random.uniform(50, 150)

                self.ping_matrix[host_id][other_host_id] = ping
                self.ping_matrix.setdefault(other_host_id, {})[host_id] = ping

                # Netzwerk-Latenz Metrik
                network_latency.labels(
                    from_host=host_id,
                    to_host=other_host_id
                ).set(ping)

        self.ping_matrix[host_id][host_id] = 0

    def get_ping(self, from_host, to_host):
        """Gibt simulierte Ping-Zeit zwischen zwei Hosts zurück"""
        return self.ping_matrix.get(from_host, {}).get(to_host, 100.0)

    def heartbeat(self, host_id):
        """Aktualisiert Heartbeat eines Hosts"""
        if host_id in self.hosts:
            self.hosts[host_id]['last_heartbeat'] = time.time()
            self.hosts[host_id]['status'] = 'online'
            redis_client.set(f'host:{host_id}', json.dumps(self.hosts[host_id]))

            # Uptime berechnen
            uptime = time.time() - self.hosts[host_id].get('registered_at', time.time())
            host_uptime.labels(
                host_id=host_id,
                location=self.hosts[host_id]['location']
            ).set(uptime)

            return True
        return False

    def update_host_resources(self, host_id, cpu_used, memory_used):
        """Aktualisiert Ressourcennutzung eines Hosts"""
        if host_id in self.hosts:
            host = self.hosts[host_id]
            host['cpu_available'] = host['cpu_capacity'] - cpu_used
            host['memory_available'] = host['memory_capacity'] - memory_used
            redis_client.set(f'host:{host_id}', json.dumps(host))

            self._update_host_metrics(host_id)
            self._update_resource_metrics()
            self._calculate_efficiency_metrics()

            return True
        return False

    def _update_host_metrics(self, host_id):
        """Aktualisiert Prometheus-Metriken für einen einzelnen Host"""
        if host_id not in self.hosts:
            return

        host = self.hosts[host_id]
        location = host['location']

        # Host-spezifische Metriken
        host_cpu_capacity.labels(host_id=host_id, location=location).set(host['cpu_capacity'])
        host_cpu_used.labels(host_id=host_id, location=location).set(
            host['cpu_capacity'] - host['cpu_available']
        )
        host_memory_capacity.labels(host_id=host_id, location=location).set(host['memory_capacity'])
        host_memory_used.labels(host_id=host_id, location=location).set(
            host['memory_capacity'] - host['memory_available']
        )
        host_tasks_completed.labels(host_id=host_id, location=location).set(host['tasks_completed'])
        host_tasks_failed.labels(host_id=host_id, location=location).set(host['tasks_failed'])

        # Aktive Shards pro Host
        active_count = sum(1 for s in self.shards.values()
                           if s.get('assigned_to') == host_id and s['status'] == 'running')
        active_shards_per_host.labels(host_id=host_id, location=location).set(active_count)

    def _update_location_metrics(self):
        """Aktualisiert Location-basierte Metriken"""
        location_counts = defaultdict(int)

        for host in self.hosts.values():
            if host['status'] == 'online':
                location_counts[host['location']] += 1

        # Setze alle bekannten Locations
        for location in set(h['location'] for h in self.hosts.values()):
            active_hosts.labels(location=location).set(location_counts[location])

    def _update_resource_metrics(self):
        """Aktualisiert Ressourcen-Metriken nach Location"""
        location_resources = defaultdict(lambda: {
            'cpu_capacity': 0, 'cpu_used': 0,
            'mem_capacity': 0, 'mem_used': 0
        })

        online_hosts = [h for h in self.hosts.values() if h['status'] == 'online']

        for host in online_hosts:
            loc = host['location']
            location_resources[loc]['cpu_capacity'] += host['cpu_capacity']
            location_resources[loc]['cpu_used'] += host['cpu_capacity'] - host['cpu_available']
            location_resources[loc]['mem_capacity'] += host['memory_capacity']
            location_resources[loc]['mem_used'] += host['memory_capacity'] - host['memory_available']

        # Metriken pro Location
        for location, res in location_resources.items():
            # CPU
            resource_capacity.labels(resource_type='cpu', location=location).set(res['cpu_capacity'])
            resource_used.labels(resource_type='cpu', location=location).set(res['cpu_used'])
            resource_available.labels(resource_type='cpu', location=location).set(
                res['cpu_capacity'] - res['cpu_used']
            )
            if res['cpu_capacity'] > 0:
                cpu_util = (res['cpu_used'] / res['cpu_capacity']) * 100
                resource_utilization.labels(resource_type='cpu', location=location).set(cpu_util)

            # Memory
            resource_capacity.labels(resource_type='memory', location=location).set(res['mem_capacity'])
            resource_used.labels(resource_type='memory', location=location).set(res['mem_used'])
            resource_available.labels(resource_type='memory', location=location).set(
                res['mem_capacity'] - res['mem_used']
            )
            if res['mem_capacity'] > 0:
                mem_util = (res['mem_used'] / res['mem_capacity']) * 100
                resource_utilization.labels(resource_type='memory', location=location).set(mem_util)

    def _calculate_efficiency_metrics(self):
        """Berechnet Effizienz- und Optimierungsmetriken"""
        online_hosts = [h for h in self.hosts.values() if h['status'] == 'online']

        if not online_hosts:
            return

        # Bin-Packing Effizienz (durchschnittliche Auslastung)
        total_efficiency = 0
        for host in online_hosts:
            cpu_util = 1 - (host['cpu_available'] / host['cpu_capacity'])
            mem_util = 1 - (host['memory_available'] / host['memory_capacity'])
            host_efficiency = (cpu_util + mem_util) / 2
            total_efficiency += host_efficiency

        avg_efficiency = total_efficiency / len(online_hosts)
        bin_packing_efficiency.set(avg_efficiency)

        # Shard-Verteilungs-Varianz
        host_shard_counts = []
        for host in online_hosts:
            count = sum(1 for s in self.shards.values()
                        if s.get('assigned_to') == host['host_id'] and s['status'] == 'running')
            host_shard_counts.append(count)

        if len(host_shard_counts) > 1:
            mean = sum(host_shard_counts) / len(host_shard_counts)
            variance = sum((x - mean) ** 2 for x in host_shard_counts) / len(host_shard_counts)
            shard_distribution_variance.set(variance)

        # Ressourcen-Fragmentierung
        for resource_type in ['cpu', 'memory']:
            fragments = []
            for host in online_hosts:
                if resource_type == 'cpu':
                    available = host['cpu_available']
                    capacity = host['cpu_capacity']
                else:
                    available = host['memory_available']
                    capacity = host['memory_capacity']

                if capacity > 0:
                    fragment_ratio = available / capacity
                    fragments.append(fragment_ratio)

            if fragments:
                # Fragmentierung = Standardabweichung der verfügbaren Ressourcen
                mean_frag = sum(fragments) / len(fragments)
                variance_frag = sum((x - mean_frag) ** 2 for x in fragments) / len(fragments)
                fragmentation_index = variance_frag ** 0.5
                resource_fragmentation.labels(resource_type=resource_type).set(fragmentation_index)

    def submit_task(self, task_data):
        """Nimmt eine neue Aufgabe entgegen"""
        self.task_counter += 1
        task_id = f"task_{self.task_counter}_{int(time.time())}"

        task = {
            'task_id': task_id,
            'description': task_data.get('description'),
            'cpu_required': task_data.get('cpu_required', 1),
            'memory_required': task_data.get('memory_required', 512),
            'duration': task_data.get('duration', 10),
            'shardable': task_data.get('shardable', True),
            'min_shards': task_data.get('min_shards', 1),
            'max_shards': task_data.get('max_shards', 10),
            'status': 'pending',
            'submitted_at': time.time(),
            'shards': [],
            'completed_shards': 0,
            'failed_shards': 0
        }

        self.tasks[task_id] = task
        redis_client.set(f'task:{task_id}', json.dumps(task))

        # Metriken
        metrics_collector.record_task_submitted(
            task_id,
            task_data.get('description', 'unknown'),
            task['shardable']
        )

        if task['shardable']:
            self._create_shards(task_id)
        else:
            redis_client.rpush('pending_tasks', task_id)

        pending_tasks.set(redis_client.llen('pending_tasks'))

        logger.info(f"Aufgabe eingereicht: {task_id} (Shardable: {task['shardable']})")
        return task_id

    def _create_shards(self, task_id):
        """Erstellt Shards für einen Task"""
        task = self.tasks[task_id]
        online_hosts = [h for h in self.hosts.values() if h['status'] == 'online']

        if not online_hosts:
            redis_client.rpush('pending_tasks', task_id)
            scheduling_failures.labels(reason='no_hosts_available').inc()
            logger.warning(f"Keine Hosts für Sharding verfügbar")
            return

        num_shards = min(
            task['max_shards'],
            max(task['min_shards'], len(online_hosts))
        )

        cpu_per_shard = task['cpu_required'] / num_shards
        memory_per_shard = task['memory_required'] / num_shards
        duration_per_shard = task['duration'] / num_shards * 1.1

        logger.info(f"Erstelle {num_shards} Shards für Task {task_id}")

        for i in range(num_shards):
            self.shard_counter += 1
            shard_id = f"{task_id}_shard_{i}"

            shard = {
                'shard_id': shard_id,
                'parent_task': task_id,
                'shard_index': i,
                'total_shards': num_shards,
                'cpu_required': cpu_per_shard,
                'memory_required': memory_per_shard,
                'duration': duration_per_shard,
                'status': 'pending',
                'assigned_to': None,
                'submitted_at': time.time(),
                'created_at': time.time()
            }

            self.shards[shard_id] = shard
            redis_client.set(f'shard:{shard_id}', json.dumps(shard))
            redis_client.rpush('pending_shards', shard_id)

            task['shards'].append(shard_id)
            shard_creations.inc()

        redis_client.set(f'task:{task_id}', json.dumps(task))
        pending_shards.set(redis_client.llen('pending_shards'))

    def find_suitable_host_bin_packing(self, cpu_req, memory_req):
        """Findet Host mit Best-Fit Bin-Packing"""
        online_hosts = [
            (host_id, host) for host_id, host in self.hosts.items()
            if host['status'] == 'online' and
               host['cpu_available'] >= cpu_req and
               host['memory_available'] >= memory_req
        ]

        if not online_hosts:
            scheduling_failures.labels(reason='insufficient_resources').inc()
            return None

        best_host = None
        best_waste = float('inf')

        for host_id, host in online_hosts:
            cpu_waste = host['cpu_available'] - cpu_req
            mem_waste = host['memory_available'] - memory_req

            cpu_waste_pct = cpu_waste / host['cpu_capacity']
            mem_waste_pct = mem_waste / host['memory_capacity']

            total_waste = (cpu_waste_pct + mem_waste_pct) / 2

            if total_waste < best_waste:
                best_waste = total_waste
                best_host = host_id

        return best_host

    def assign_shard(self, shard_id, host_id):
        """Weist einen Shard einem Host zu"""
        if shard_id not in self.shards or host_id not in self.hosts:
            return False

        shard = self.shards[shard_id]
        host = self.hosts[host_id]

        shard['assigned_to'] = host_id
        shard['status'] = 'assigned'
        shard['assigned_at'] = time.time()

        host['cpu_available'] -= shard['cpu_required']
        host['memory_available'] -= shard['memory_required']

        redis_client.set(f'shard:{shard_id}', json.dumps(shard))
        redis_client.set(f'host:{host_id}', json.dumps(host))

        # Metriken
        task_assignments.inc()
        metrics_collector.record_shard_assigned(shard_id, shard.get('created_at', time.time()))

        # Cross-Region Tracking
        task = self.tasks.get(shard['parent_task'])
        if task:
            task_location = 'client'
            host_location = host['location']
            if task_location != host_location:
                cross_region_assignments.labels(
                    from_region=task_location,
                    to_region=host_location
                ).inc()

        self._update_host_metrics(host_id)
        self._update_resource_metrics()

        logger.info(f"Shard {shard_id} zugewiesen an {host_id}")
        return True

    def complete_shard(self, shard_id, success=True):
        """Markiert einen Shard als abgeschlossen"""
        if shard_id not in self.shards:
            return False

        shard = self.shards[shard_id]
        host_id = shard['assigned_to']
        parent_task_id = shard['parent_task']

        shard['status'] = 'completed' if success else 'failed'
        shard['completed_at'] = time.time()

        # Ressourcen freigeben
        if host_id and host_id in self.hosts:
            host = self.hosts[host_id]
            host['cpu_available'] += shard['cpu_required']
            host['memory_available'] += shard['memory_required']

            if success:
                host['tasks_completed'] += 1
            else:
                host['tasks_failed'] += 1

            redis_client.set(f'host:{host_id}', json.dumps(host))
            self._update_host_metrics(host_id)

        redis_client.set(f'shard:{shard_id}', json.dumps(shard))

        # Metriken
        metrics_collector.record_shard_completed(
            shard_id,
            self.hosts[host_id]['location'] if host_id in self.hosts else 'unknown',
            success
        )

        # Update Parent Task
        if parent_task_id in self.tasks:
            task = self.tasks[parent_task_id]
            if success:
                task['completed_shards'] += 1
            else:
                task['failed_shards'] += 1

            total_done = task['completed_shards'] + task['failed_shards']
            if total_done == len(task['shards']):
                if task['failed_shards'] == 0:
                    task['status'] = 'completed'
                    status = 'completed'
                else:
                    task['status'] = 'partially_failed'
                    status = 'partially_failed'

                task['completed_at'] = time.time()

                # Task Completion Metriken
                metrics_collector.record_task_completed(
                    parent_task_id,
                    task.get('description', 'unknown'),
                    len(task['shards']) > 1,
                    status
                )

            redis_client.set(f'task:{parent_task_id}', json.dumps(task))

        self._update_resource_metrics()
        self._update_shard_counts()

        logger.info(f"Shard {shard_id} abgeschlossen: {'Erfolg' if success else 'Fehlgeschlagen'}")
        return True

    def _update_shard_counts(self):
        """Aktualisiert Shard-Zähler"""
        pending = sum(1 for s in self.shards.values() if s['status'] == 'pending')
        running = sum(1 for s in self.shards.values() if s['status'] in ['assigned', 'running'])

        pending_shards.set(pending)
        running_shards.set(running)

    def check_host_health(self):
        """Überprüft Health der Hosts"""
        current_time = time.time()

        for host_id, host in list(self.hosts.items()):
            if current_time - host['last_heartbeat'] > self.heartbeat_timeout:
                if host['status'] == 'online':
                    logger.warning(f"Host {host_id} nicht mehr erreichbar")
                    host['status'] = 'offline'
                    redis_client.set(f'host:{host_id}', json.dumps(host))

                    host_failures.labels(
                        host_id=host_id,
                        location=host['location']
                    ).inc()

                    self.reassign_shards_from_host(host_id)

        self._update_location_metrics()
        self._update_resource_metrics()

    def reassign_shards_from_host(self, failed_host_id):
        """Weist Shards von ausgefallenen Hosts neu zu"""
        for shard_id, shard in self.shards.items():
            if shard['assigned_to'] == failed_host_id and shard['status'] in ['assigned', 'running']:
                logger.info(f"Shard {shard_id} wird neu zugewiesen")

                shard['status'] = 'pending'
                shard['assigned_to'] = None
                redis_client.set(f'shard:{shard_id}', json.dumps(shard))
                redis_client.rpush('pending_shards', shard_id)

                shard_reassignments.labels(reason='host_failure').inc()

    def schedule_shards(self):
        """Scheduler für Shard-Zuweisung"""
        while True:
            try:
                shard_id = redis_client.lpop('pending_shards')

                if shard_id:
                    shard = self.shards.get(shard_id)
                    if shard:
                        host_id = self.find_suitable_host_bin_packing(
                            shard['cpu_required'],
                            shard['memory_required']
                        )

                        if host_id:
                            self.assign_shard(shard_id, host_id)
                        else:
                            redis_client.rpush('pending_shards', shard_id)
                            time.sleep(5)

                self._update_shard_counts()
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Scheduler-Fehler: {e}")
                time.sleep(5)


middleman = ResourceMiddleman()


# REST API Endpoints

@app.route('/register', methods=['POST'])
def register_host():
    data = request.json
    middleman.register_host(data)
    return jsonify({'status': 'registered', 'host_id': data['host_id']}), 200


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = request.json
    host_id = data.get('host_id')

    if middleman.heartbeat(host_id):
        if 'cpu_used' in data and 'memory_used' in data:
            middleman.update_host_resources(host_id, data['cpu_used'], data['memory_used'])
        return jsonify({'status': 'ok'}), 200

    return jsonify({'error': 'Host nicht gefunden'}), 404


@app.route('/submit_task', methods=['POST'])
def submit_task():
    data = request.json
    task_id = middleman.submit_task(data)
    return jsonify({'task_id': task_id, 'status': 'submitted'}), 201


@app.route('/task/<task_id>', methods=['GET'])
def get_task(task_id):
    task = middleman.tasks.get(task_id)
    if task:
        return jsonify(task), 200
    return jsonify({'error': 'Task nicht gefunden'}), 404


@app.route('/shard/<shard_id>/complete', methods=['POST'])
def complete_shard(shard_id):
    data = request.json
    success = data.get('success', True)

    if middleman.complete_shard(shard_id, success):
        return jsonify({'status': 'completed'}), 200
    return jsonify({'error': 'Shard nicht gefunden'}), 404


@app.route('/hosts', methods=['GET'])
def list_hosts():
    return jsonify(list(middleman.hosts.values())), 200


@app.route('/tasks', methods=['GET'])
def list_tasks():
    return jsonify(list(middleman.tasks.values())), 200


@app.route('/shards', methods=['GET'])
def list_shards():
    return jsonify(list(middleman.shards.values())), 200


@app.route('/ping/<from_host>/<to_host>', methods=['GET'])
def get_ping(from_host, to_host):
    ping = middleman.get_ping(from_host, to_host)
    return jsonify({'from': from_host, 'to': to_host, 'ping_ms': ping}), 200


@app.route('/metrics', methods=['GET'])
def metrics():
    return generate_latest(), 200


@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'active_hosts': len([h for h in middleman.hosts.values() if h['status'] == 'online']),
        'total_hosts': len(middleman.hosts),
        'pending_shards': redis_client.llen('pending_shards'),
        'total_tasks': len(middleman.tasks),
        'total_shards': len(middleman.shards)
    }), 200


def health_check_loop():
    while True:
        middleman.check_host_health()
        time.sleep(10)


def scheduler_loop():
    middleman.schedule_shards()


def metrics_update_loop():
    """Periodisches Update von berechneten Metriken"""
    while True:
        try:
            middleman._update_resource_metrics()
            middleman._calculate_efficiency_metrics()
            middleman._update_shard_counts()
        except Exception as e:
            logger.error(f"Metrics update error: {e}")
        time.sleep(15)


if __name__ == '__main__':
    os.makedirs('logs', exist_ok=True)

    threading.Thread(target=health_check_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    threading.Thread(target=metrics_update_loop, daemon=True).start()

    logger.info("Middleman mit erweiterten Metriken gestartet")
    app.run(host='0.0.0.0', port=5000, debug=False)