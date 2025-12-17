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
        self.middleman_url = os.getenv('MIDDLEMAN_URL', 'http://middleman:8000')
        self.task_interval = int(os.getenv('TASK_INTERVAL', 30))
        self.task_types = [
            {'name': 'Datenverarbeitung', 'cpu': 2, 'mem': 1024, 'duration': 15},
            {'name': 'Video-Encoding', 'cpu': 4, 'mem': 2048, 'duration': 30},
            {'name': 'ML-Training', 'cpu': 8, 'mem': 4096, 'duration': 45},
            {'name': 'Bildanalyse', 'cpu': 2, 'mem': 512, 'duration': 10},
            {'name': 'Batch-Processing', 'cpu': 1, 'mem': 256, 'duration': 5},
        ]

    def submit_task(self, task_type):
        "Reicht eine Aufgabe ein"
        try:
            response = requests.post(
                f"{self.middleman_url}/submit_task",
                json={
                    'description': task_type['name'],
                    'cpu_required': task_type['cpu'],
                    'memory_required': task_type['mem'],
                    'duration': task_type['duration']
                },
                timeout=5
            )

            if response.status_code == 201:
                task_id = response.json()['task_id']
                logger.info(f"Aufgabe eingereicht: {task_id} - {task_type['name']}")
                return task_id
            else:
                logger.error(f"Fehler beim Einreichen: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Fehler beim Einreichen: {e}")
            return None

    def check_task_status(self, task_id):
        "Prüft Status einer Aufgabe"
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
        "Holt Health-Status"
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

    def run(self):
        "Generiert regelmäßig die Aufgaben"
        logger.info("Client gestartet . Warte auf Mittelmann...")

        # Warte auf Middleman
        while True:
            health = self.get_middleman_health()
            if health:
                logger.info(f"Mittelmann bereit: {health}")
                break
            time.sleep(5)

        task_counter = 0
        submitted_tasks = {}

        while True:
            # Neuen Task generieren
            task_type = random.choice(self.task_types)
            task_id = self.submit_task(task_type)

            if task_id:
                task_counter += 1
                submitted_tasks[task_id] = {
                    'type': task_type['name'],
                    'submitted_at': time.time()
                }

            # Status von alten Aufgaben prüfen
            completed = []
            for tid, info in submitted_tasks.items():
                status = self.check_task_status(tid)
                if status and status['status'] in ['completed', 'failed']:
                    duration = time.time() - info['submitted_at']
                    logger.info(f"Task {tid} abgeschlossen: {status['status']} "
                                f"nach {duration:.1f}s")
                    completed.append(tid)

            # Abgeschlossene Taks entfernen
            for tid in completed:
                del submitted_tasks[tid]

            # Statistiken
            health = self.get_middleman_health()
            if health:
                logger.info(f"Statistiken - Eingereicht: {task_counter}, "
                            f"Pending: {health['pending_tasks']}, "
                            f"Active Hosts: {health['active_hosts']}")

            time.sleep(self.task_interval)


if __name__ == '__main__':
    client = TaskClient()
    client.run()