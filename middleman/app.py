from flask import Flask, request, jsonify
import redis
import json
import time
import threading
from datetime import datetime, timedelta
from prometheus_client import Counter, Gauge, Histogram, generate_latest
import logging
import os

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
active_hosts = Gauge('active_hosts', 'Number of active hosts')
pending_tasks = Gauge('pending_tasks', 'Number of pending tasks')
task_completion_time = Histogram('task_completion_seconds', 'Task completion time')

class ResourceMiddleman:
    def __init__(self):
        self.hosts = {}  # host_id -> host_info
        self.tasks = {}  # task_id -> task_info
        self.task_counter = 0
        self.heartbeat_timeout = 30  # Sekunden
        
    def register_host(self, host_data):
        """Registriert einen neuen Host"""
        host_id = host_data.get('host_id')
        
        host_info = {
            'host_id': host_id,
            'name': host_data.get('name'),
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
        
        host_registrations.inc()
        active_hosts.set(len([h for h in self.hosts.values() if h['status'] == 'online']))
        
        logger.info(f"Host registriert: {host_id} ({host_info['name']})")
        return True
    
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
            return True
        return False
    
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
            'status': 'pending',
            'submitted_at': time.time(),
            'assigned_to': None,
            'replicas': []
        }
        
        self.tasks[task_id] = task
        redis_client.set(f'task:{task_id}', json.dumps(task))
        redis_client.rpush('pending_tasks', task_id)
        
        task_submissions.inc()
        pending_tasks.set(redis_client.llen('pending_tasks'))
        
        logger.info(f"Aufgabe eingereicht: {task_id}")
        return task_id
    
    def find_suitable_host(self, cpu_req, memory_req):
        """Findet einen passenden Host für die Aufgabe (Load Balancing)"""
        suitable_hosts = []
        
        for host_id, host in self.hosts.items():
            if (host['status'] == 'online' and
                host['cpu_available'] >= cpu_req and
                host['memory_available'] >= memory_req):
                
                # Score basierend auf verfügbaren Ressourcen
                cpu_score = host['cpu_available'] / host['cpu_capacity']
                mem_score = host['memory_available'] / host['memory_capacity']
                score = (cpu_score + mem_score) / 2
                
                suitable_hosts.append((host_id, score))
        
        if not suitable_hosts:
            return None
        
        # Sortiere nach Score (höchster Score = am meisten frei)
        suitable_hosts.sort(key=lambda x: x[1], reverse=True)
        return suitable_hosts[0][0]
    
    def assign_task(self, task_id, host_id):
        """Weist eine Aufgabe einem Host zu"""
        if task_id not in self.tasks or host_id not in self.hosts:
            return False
        
        task = self.tasks[task_id]
        task['assigned_to'] = host_id
        task['status'] = 'assigned'
        task['assigned_at'] = time.time()
        
        # Ressourcen reservieren
        host = self.hosts[host_id]
        host['cpu_available'] -= task['cpu_required']
        host['memory_available'] -= task['memory_required']
        
        redis_client.set(f'task:{task_id}', json.dumps(task))
        redis_client.set(f'host:{host_id}', json.dumps(host))
        
        task_assignments.inc()
        
        logger.info(f"Aufgabe {task_id} zugewiesen an {host_id}")
        return True
    
    def complete_task(self, task_id, success=True):
        """Markiert eine Aufgabe als abgeschlossen"""
        if task_id not in self.tasks:
            return False
        
        task = self.tasks[task_id]
        host_id = task['assigned_to']
        
        task['status'] = 'completed' if success else 'failed'
        task['completed_at'] = time.time()
        
        # Metriken
        if success and 'submitted_at' in task:
            duration = task['completed_at'] - task['submitted_at']
            task_completion_time.observe(duration)
        
        # Ressourcen freigeben
        if host_id and host_id in self.hosts:
            host = self.hosts[host_id]
            host['cpu_available'] += task['cpu_required']
            host['memory_available'] += task['memory_required']
            
            if success:
                host['tasks_completed'] += 1
            else:
                host['tasks_failed'] += 1
            
            redis_client.set(f'host:{host_id}', json.dumps(host))
        
        redis_client.set(f'task:{task_id}', json.dumps(task))
        
        logger.info(f"Aufgabe {task_id} abgeschlossen: {'Erfolg' if success else 'Fehlgeschlagen'}")
        return True
    
    def check_host_health(self):
        """Überprüft Health der Hosts (Heartbeat-Monitoring)"""
        current_time = time.time()
        
        for host_id, host in list(self.hosts.items()):
            if current_time - host['last_heartbeat'] > self.heartbeat_timeout:
                if host['status'] == 'online':
                    logger.warning(f"Host {host_id} nicht mehr erreichbar")
                    host['status'] = 'offline'
                    redis_client.set(f'host:{host_id}', json.dumps(host))
                    
                    # Aufgaben neu zuweisen
                    self.reassign_tasks_from_host(host_id)
        
        active_hosts.set(len([h for h in self.hosts.values() if h['status'] == 'online']))
    
    def reassign_tasks_from_host(self, failed_host_id):
        """Weist Aufgaben von ausgefallenen Hosts neu zu"""
        for task_id, task in self.tasks.items():
            if task['assigned_to'] == failed_host_id and task['status'] in ['assigned', 'running']:
                logger.info(f"Aufgabe {task_id} wird neu zugewiesen (Host {failed_host_id} ausgefallen)")
                
                # Task zurück in Queue
                task['status'] = 'pending'
                task['assigned_to'] = None
                redis_client.set(f'task:{task_id}', json.dumps(task))
                redis_client.rpush('pending_tasks', task_id)
    
    def schedule_tasks(self):
        """Scheduler für Aufgabenzuweisung"""
        while True:
            try:
                task_id = redis_client.lpop('pending_tasks')
                
                if task_id:
                    task = self.tasks.get(task_id)
                    if task:
                        host_id = self.find_suitable_host(
                            task['cpu_required'],
                            task['memory_required']
                        )
                        
                        if host_id:
                            self.assign_task(task_id, host_id)
                        else:
                            # Zurück in Queue wenn kein Host verfügbar
                            redis_client.rpush('pending_tasks', task_id)
                            time.sleep(5)
                
                pending_tasks.set(redis_client.llen('pending_tasks'))
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Scheduler-Fehler: {e}")
                time.sleep(5)

middleman = ResourceMiddleman()

# REST API Endpoints

@app.route('/register', methods=['POST'])
def register_host():
    """Host-Registrierung"""
    data = request.json
    middleman.register_host(data)
    return jsonify({'status': 'registered', 'host_id': data['host_id']}), 200

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    """Heartbeat von Hosts"""
    data = request.json
    host_id = data.get('host_id')
    
    if middleman.heartbeat(host_id):
        # Update Ressourcen wenn mitgeliefert
        if 'cpu_used' in data and 'memory_used' in data:
            middleman.update_host_resources(host_id, data['cpu_used'], data['memory_used'])
        return jsonify({'status': 'ok'}), 200
    
    return jsonify({'error': 'Host nicht gefunden'}), 404

@app.route('/submit_task', methods=['POST'])
def submit_task():
    """Aufgabe einreichen"""
    data = request.json
    task_id = middleman.submit_task(data)
    return jsonify({'task_id': task_id, 'status': 'submitted'}), 201

@app.route('/task/<task_id>', methods=['GET'])
def get_task(task_id):
    "Task-Status abfragen"
    task = middleman.tasks.get(task_id)
    if task:
        return jsonify(task), 200
    return jsonify({'error': 'Task nicht gefunden'}), 404

@app.route('/task/<task_id>/complete', methods=['POST'])
def complete_task(task_id):
    "Task als abgeschlossen markieren"
    data = request.json
    success = data.get('success', True)
    
    if middleman.complete_task(task_id, success):
        return jsonify({'status': 'completed'}), 200
    return jsonify({'error': 'Task nicht gefunden'}), 404

@app.route('/hosts', methods=['GET'])
def list_hosts():
    "Alle Hosts auflisten"
    return jsonify(list(middleman.hosts.values())), 200

@app.route('/tasks', methods=['GET'])
def list_tasks():
    "Alle Tasks auflisten"
    return jsonify(list(middleman.tasks.values())), 200

@app.route('/metrics', methods=['GET'])
def metrics():

    return generate_latest(), 200

@app.route('/health', methods=['GET'])
def health():
    "Health Check"
    return jsonify({
        'status': 'healthy',
        'active_hosts': len([h for h in middleman.hosts.values() if h['status'] == 'online']),
        'total_hosts': len(middleman.hosts),
        'pending_tasks': redis_client.llen('pending_tasks'),
        'total_tasks': len(middleman.tasks)
    }), 200

def health_check_loop():
    while True:
        middleman.check_host_health()
        time.sleep(10)

def scheduler_loop():
    middleman.schedule_tasks()

if __name__ == '__main__':
    # Logs-Verzeichnis erstellen
    os.makedirs('logs', exist_ok=True)
    
    # Background-Threads starten
    threading.Thread(target=health_check_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    
    logger.info("Middleman gestartet")
    app.run(host='0.0.0.0', port=5000, debug=False)