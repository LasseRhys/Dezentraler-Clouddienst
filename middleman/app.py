from flask import Flask, request, jsonify
import redis
import json
import time
import threading
from datetime import datetime, timedelta
from prometheus_client import Counter, Gauge, Histogram, generate_latest
import logging
import os
import random

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

# Prometheus Metriken
host_registrations = Counter('host_registrations_total', 'Total host registrations')
task_submissions = Counter('task_submissions_total', 'Total task submissions')
task_assignments = Counter('task_assignments_total', 'Total task assignments')
shard_creations = Counter('task_shards_created_total', 'Total task shards created')
active_hosts = Gauge('active_hosts', 'Number of active hosts')
pending_tasks = Gauge('pending_tasks', 'Number of pending tasks')
task_completion_time = Histogram('task_completion_seconds', 'Task completion time')
resource_utilization = Gauge('resource_utilization_percent', 'Average resource utilization', ['resource_type'])


class ResourceMiddleman:
    def __init__(self):
        self.hosts = {}  # host_id -> host_info
        self.tasks = {}  # task_id -> task_info
        self.shards = {}  # shard_id -> shard_info
        self.task_counter = 0
        self.shard_counter = 0
        self.heartbeat_timeout = 30
        self.ping_matrix = {}  # Simulierte Netzwerklatenz zwischen Hosts

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
            'tasks_completed': 0,
            'tasks_failed': 0,
            'endpoint': host_data.get('endpoint')
        }

        self.hosts[host_id] = host_info
        redis_client.set(f'host:{host_id}', json.dumps(host_info))

        # Initialisiere Ping-Matrix für neuen Host
        self._initialize_ping_for_host(host_id)

        host_registrations.inc()
        active_hosts.set(len([h for h in self.hosts.values() if h['status'] == 'online']))

        logger.info(f"Host registriert: {host_id} ({host_info['name']}) in {host_info['location']}")
        return True

    def _initialize_ping_for_host(self, host_id):
        """Initialisiert simulierte Ping-Zeiten für einen Host"""
        if host_id not in self.ping_matrix:
            self.ping_matrix[host_id] = {}

        for other_host_id in self.hosts.keys():
            if other_host_id != host_id:
                # Simuliere Ping basierend auf "Distanz" zwischen Hosts
                # Gleiche Location = 5-15ms, unterschiedlich = 50-150ms
                if self.hosts[host_id]['location'] == self.hosts[other_host_id]['location']:
                    ping = random.uniform(5, 15)
                else:
                    ping = random.uniform(50, 150)

                self.ping_matrix[host_id][other_host_id] = ping
                self.ping_matrix.setdefault(other_host_id, {})[host_id] = ping

        # Ping zu sich selbst ist 0
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
            return True
        return False

    def update_host_resources(self, host_id, cpu_used, memory_used):
        """Aktualisiert Ressourcennutzung eines Hosts"""
        if host_id in self.hosts:
            host = self.hosts[host_id]
            host['cpu_available'] = host['cpu_capacity'] - cpu_used
            host['memory_available'] = host['memory_capacity'] - memory_used
            redis_client.set(f'host:{host_id}', json.dumps(host))

            # Update Metriken
            self._update_resource_metrics()
            return True
        return False

    def _update_resource_metrics(self):
        """Aktualisiert Prometheus-Metriken für Ressourcenauslastung"""
        online_hosts = [h for h in self.hosts.values() if h['status'] == 'online']
        if not online_hosts:
            return

        total_cpu_capacity = sum(h['cpu_capacity'] for h in online_hosts)
        total_cpu_used = sum(h['cpu_capacity'] - h['cpu_available'] for h in online_hosts)

        total_mem_capacity = sum(h['memory_capacity'] for h in online_hosts)
        total_mem_used = sum(h['memory_capacity'] - h['memory_available'] for h in online_hosts)

        cpu_util = (total_cpu_used / total_cpu_capacity * 100) if total_cpu_capacity > 0 else 0
        mem_util = (total_mem_used / total_mem_capacity * 100) if total_mem_capacity > 0 else 0

        resource_utilization.labels(resource_type='cpu').set(cpu_util)
        resource_utilization.labels(resource_type='memory').set(mem_util)

    def submit_task(self, task_data):
        """Nimmt eine neue Aufgabe entgegen und erstellt Shards falls nötig"""
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

        # Erstelle Shards wenn Task shardable ist
        if task['shardable']:
            self._create_shards(task_id)
        else:
            # Nicht-shardbare Tasks direkt in Queue
            redis_client.rpush('pending_tasks', task_id)

        task_submissions.inc()
        pending_tasks.set(redis_client.llen('pending_tasks'))

        logger.info(f"Aufgabe eingereicht: {task_id} (Shardable: {task['shardable']})")
        return task_id

    def _create_shards(self, task_id):
        """Erstellt Shards für einen Task basierend auf verfügbaren Hosts"""
        task = self.tasks[task_id]

        # Berechne optimale Anzahl Shards basierend auf verfügbaren Hosts
        online_hosts = [h for h in self.hosts.values() if h['status'] == 'online']

        if not online_hosts:
            # Keine Hosts verfügbar, Task als ganzes in Queue
            redis_client.rpush('pending_tasks', task_id)
            logger.warning(f"Keine Hosts für Sharding verfügbar, Task {task_id} als Ganzes eingeplant")
            return

        # Anzahl Shards zwischen min und verfügbaren Hosts
        num_shards = min(
            task['max_shards'],
            max(task['min_shards'], len(online_hosts))
        )

        # CPU und Memory pro Shard (mit kleinem Overhead)
        cpu_per_shard = task['cpu_required'] / num_shards
        memory_per_shard = task['memory_required'] / num_shards
        duration_per_shard = task['duration'] / num_shards * 1.1  # 10% Overhead für Koordination

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
                'submitted_at': time.time()
            }

            self.shards[shard_id] = shard
            redis_client.set(f'shard:{shard_id}', json.dumps(shard))
            redis_client.rpush('pending_shards', shard_id)

            task['shards'].append(shard_id)
            shard_creations.inc()

        # Update Task
        redis_client.set(f'task:{task_id}', json.dumps(task))
        logger.info(f"Task {task_id} in {num_shards} Shards aufgeteilt")

    def find_suitable_host_bin_packing(self, cpu_req, memory_req):
        """
        Findet Host mit Best-Fit Bin-Packing Strategie.
        Ziel: Maximale Auslastung, minimiere verschwendete Ressourcen.
        """
        online_hosts = [
            (host_id, host) for host_id, host in self.hosts.items()
            if host['status'] == 'online' and
               host['cpu_available'] >= cpu_req and
               host['memory_available'] >= memory_req
        ]

        if not online_hosts:
            return None

        best_host = None
        best_waste = float('inf')

        for host_id, host in online_hosts:
            # Berechne "Verschwendung" nach Zuweisung
            cpu_waste = host['cpu_available'] - cpu_req
            mem_waste = host['memory_available'] - memory_req

            # Normalisiere auf Prozentsatz der Kapazität
            cpu_waste_pct = cpu_waste / host['cpu_capacity']
            mem_waste_pct = mem_waste / host['memory_capacity']

            # Gesamte Verschwendung (niedrig = besser)
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
        shard['assigned_to'] = host_id
        shard['status'] = 'assigned'
        shard['assigned_at'] = time.time()

        # Ressourcen reservieren
        host = self.hosts[host_id]
        host['cpu_available'] -= shard['cpu_required']
        host['memory_available'] -= shard['memory_required']

        redis_client.set(f'shard:{shard_id}', json.dumps(shard))
        redis_client.set(f'host:{host_id}', json.dumps(host))

        task_assignments.inc()

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

        redis_client.set(f'shard:{shard_id}', json.dumps(shard))

        # Update Parent Task
        if parent_task_id in self.tasks:
            task = self.tasks[parent_task_id]
            if success:
                task['completed_shards'] += 1
            else:
                task['failed_shards'] += 1

            # Prüfe ob alle Shards fertig
            total_done = task['completed_shards'] + task['failed_shards']
            if total_done == len(task['shards']):
                if task['failed_shards'] == 0:
                    task['status'] = 'completed'
                    logger.info(f"Task {parent_task_id} vollständig abgeschlossen")
                else:
                    task['status'] = 'partially_failed'
                    logger.warning(f"Task {parent_task_id} mit {task['failed_shards']} fehlgeschlagenen Shards")

                task['completed_at'] = time.time()

                # Metriken
                if 'submitted_at' in task:
                    duration = task['completed_at'] - task['submitted_at']
                    task_completion_time.observe(duration)

            redis_client.set(f'task:{parent_task_id}', json.dumps(task))

        logger.info(f"Shard {shard_id} abgeschlossen: {'Erfolg' if success else 'Fehlgeschlagen'}")
        return True

    def check_host_health(self):
        """Überprüft Health der Hosts"""
        current_time = time.time()

        for host_id, host in list(self.hosts.items()):
            if current_time - host['last_heartbeat'] > self.heartbeat_timeout:
                if host['status'] == 'online':
                    logger.warning(f"Host {host_id} nicht mehr erreichbar")
                    host['status'] = 'offline'
                    redis_client.set(f'host:{host_id}', json.dumps(host))

                    # Shards neu zuweisen
                    self.reassign_shards_from_host(host_id)

        active_hosts.set(len([h for h in self.hosts.values() if h['status'] == 'online']))

    def reassign_shards_from_host(self, failed_host_id):
        """Weist Shards von ausgefallenen Hosts neu zu"""
        for shard_id, shard in self.shards.items():
            if shard['assigned_to'] == failed_host_id and shard['status'] in ['assigned', 'running']:
                logger.info(f"Shard {shard_id} wird neu zugewiesen (Host {failed_host_id} ausgefallen)")

                # Shard zurück in Queue
                shard['status'] = 'pending'
                shard['assigned_to'] = None
                redis_client.set(f'shard:{shard_id}', json.dumps(shard))
                redis_client.rpush('pending_shards', shard_id)

    def schedule_shards(self):
        """Scheduler für Shard-Zuweisung mit Bin-Packing"""
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
                            # Zurück in Queue wenn kein Host verfügbar
                            redis_client.rpush('pending_shards', shard_id)
                            time.sleep(5)

                pending_tasks.set(redis_client.llen('pending_shards'))
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


if __name__ == '__main__':
    os.makedirs('logs', exist_ok=True)

    threading.Thread(target=health_check_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()

    logger.info("Middleman")
    app.run(host='0.0.0.0', port=5000, debug=False)