import requests
import time
import random
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaskClient:
    def __init__(self):
        self.middleman_url = os.getenv('MIDDLEMAN_URL', 'http://middleman:5000')
        self.task_interval = int(os.getenv('TASK_INTERVAL', 30))

        self.task_types = [
            {
                'name': 'Kleine Datenverarbeitung',
                'cpu': 1,
                'mem': 512,
                'duration': 10,
                'shardable': False
            },
            {
                'name': 'Videoverarbeitung',
                'cpu': 8,
                'mem': 4096,
                'duration': 60,
                'shardable': True,
                'min_shards': 2,
                'max_shards': 8
            },
            {
                'name': 'ML-Training (groß)',
                'cpu': 16,
                'mem': 8192,
                'duration': 120,
                'shardable': True,
                'min_shards': 4,
                'max_shards': 10
            },
            {
                'name': 'Bildanalyse',
                'cpu': 6,
                'mem': 3072,
                'duration': 45,
                'shardable': True,
                'min_shards': 3,
                'max_shards': 6
            },
            {
                'name': 'Echtzeit-Stream',
                'cpu': 2,
                'mem': 1024,
                'duration': 15,
                'shardable': False
            },
            {
                'name': 'Datenbank-Backup',
                'cpu': 4,
                'mem': 2048,
                'duration': 30,
                'shardable': True,
                'min_shards': 2,
                'max_shards': 4
            },
            {
                'name': 'MapReduce Job',
                'cpu': 12,
                'mem': 6144,
                'duration': 90,
                'shardable': True,
                'min_shards': 6,
                'max_shards': 12
            }
        ]

    def submit_task(self, task_type):
        try:
            task_data = {
                'description': task_type['name'],
                'cpu_required': task_type['cpu'],
                'memory_required': task_type['mem'],
                'duration': task_type['duration'],
                'shardable': task_type['shardable']
            }

            if task_type['shardable']:
                task_data['min_shards'] = task_type['min_shards']
                task_data['max_shards'] = task_type['max_shards']

            response = requests.post(
                f"{self.middleman_url}/submit_task",
                json=task_data,
                timeout=5
            )

            if response.status_code == 201:
                task_id = response.json()['task_id']
                shard_info = f" (Shardable: {task_type['min_shards']}-{task_type['max_shards']})" if task_type[
                    'shardable'] else ""
                logger.info(f"Aufgabe eingereicht: {task_id} - {task_type['name']}{shard_info}")
                return task_id
            else:
                logger.error(f"Fehler beim Einreichen: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Fehler beim Einreichen: {e}")
            return None

    def check_task_status(self, task_id):
        try:
            response = requests.get(
                f"{self.middleman_url}/task/{task_id}",
                timeout=5
            )

            if response.status_code == 200:
                return response.json()

        except Exception as e:
            logger.error(f"Fehler beim Statuscheck: {e}")

        return None

    def get_middleman_health(self):
        try:
            response = requests.get(
                f"{self.middleman_url}/health",
                timeout=5
            )

            if response.status_code == 200:
                return response.json()

        except Exception as e:
            logger.error(f"Fehler beim Health-Check: {e}")

        return None

    def get_resource_utilization(self):
        try:
            response = requests.get(
                f"{self.middleman_url}/hosts",
                timeout=5
            )

            if response.status_code == 200:
                hosts = response.json()
                online_hosts = [h for h in hosts if h['status'] == 'online']

                if not online_hosts:
                    return 0, 0

                total_cpu_capacity = sum(h['cpu_capacity'] for h in online_hosts)
                total_cpu_used = sum(h['cpu_capacity'] - h['cpu_available'] for h in online_hosts)

                total_mem_capacity = sum(h['memory_capacity'] for h in online_hosts)
                total_mem_used = sum(h['memory_capacity'] - h['memory_available'] for h in online_hosts)

                cpu_util = (total_cpu_used / total_cpu_capacity * 100) if total_cpu_capacity > 0 else 0
                mem_util = (total_mem_used / total_mem_capacity * 100) if total_mem_capacity > 0 else 0

                return cpu_util, mem_util

        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Auslastung: {e}")

        return 0, 0

    def run(self):

        logger.info("Client gestartet")

        while True:
            health = self.get_middleman_health()
            if health:
                logger.info(f"Middleman bereit: {health}")
                break
            time.sleep(5)

        task_counter = 0
        submitted_tasks = {}

        while True:
            if random.random() < 0.7:  # 70proz shardbare Tasks
                task_type = random.choice([t for t in self.task_types if t['shardable']])
            else:
                task_type = random.choice(self.task_types)

            task_id = self.submit_task(task_type)

            if task_id:
                task_counter += 1
                submitted_tasks[task_id] = {
                    'type': task_type['name'],
                    'submitted_at': time.time(),
                    'shardable': task_type['shardable']
                }

            completed = []
            for tid, info in submitted_tasks.items():
                status = self.check_task_status(tid)
                if status and status['status'] in ['completed', 'failed', 'partially_failed']:
                    duration = time.time() - info['submitted_at']

                    if info['shardable'] and 'shards' in status:
                        logger.info(f"Task {tid} abgeschlossen: {status['status']} "
                                    f"nach {duration:.1f}s "
                                    f"({status['completed_shards']}/{len(status['shards'])} Shards erfolgreich)")
                    else:
                        logger.info(f"Task {tid} abgeschlossen: {status['status']} "
                                    f"nach {duration:.1f}s")

                    completed.append(tid)

            for tid in completed:
                del submitted_tasks[tid]

            health = self.get_middleman_health()
            cpu_util, mem_util = self.get_resource_utilization()

            if health:
                logger.info(f"Statistiken - "
                            f"Eingereicht: {task_counter}, "
                            f"Pending: {health.get('pending_shards', 0)} Shards, "
                            f"Active Hosts: {health['active_hosts']}/{health['total_hosts']}, "
                            f"CPU: {cpu_util:.1f}%, RAM: {mem_util:.1f}%")

            time.sleep(self.task_interval)


if __name__ == '__main__':
    client = TaskClient()
    client.run()