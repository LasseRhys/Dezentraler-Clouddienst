import requests
import time
import random
import os
import logging
import threading
import json
from datetime import datetime

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProviderHost:
    def __init__(self):
        self.host_id = os.getenv('HOST_ID', 'host_unknown')
        self.name = os.getenv('HOST_NAME', 'Unknown Host')
        self.location = os.getenv('LOCATION', 'unknown')
        self.middleman_url = os.getenv('MIDDLEMAN_URL', 'http://middleman:5000')
        self.cpu_capacity = int(os.getenv('CPU_CAPACITY', 4))
        self.memory_capacity = int(os.getenv('MEMORY_CAPACITY', 8192))
        self.uptime_probability = float(os.getenv('UPTIME_PROBABILITY', 0.95))
        self.failure_simulation = os.getenv('FAILURE_SIMULATION', 'true').lower() == 'true'

        self.cpu_used = 0
        self.memory_used = 0
        self.is_online = True
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.active_shards = {}  # thread

        logger.info(f"Host initialisiert: {self.host_id} ({self.name}) in {self.location}")
        logger.info(f"Kapazität: {self.cpu_capacity} CPU, {self.memory_capacity} MB RAM")
        logger.info(f"Uptime-Wahrscheinlichkeit: {self.uptime_probability * 100}%")

    def register(self):
        try:
            response = requests.post(
                f"{self.middleman_url}/register",
                json={
                    'host_id': self.host_id,
                    'name': self.name,
                    'location': self.location,
                    'cpu_capacity': self.cpu_capacity,
                    'memory_capacity': self.memory_capacity,
                    'endpoint': f'http://{self.host_id}:8000'
                },
                timeout=5
            )

            if response.status_code == 200:
                logger.info(f"Erfolgreich beim Middleman registriert")
                return True
            else:
                logger.error(f"Registrierung fehlgeschlagen: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Registrierung fehlgeschlagen: {e}")
            return False

    def send_heartbeat(self):

        if not self.is_online:
            return False

        try:
            response = requests.post(
                f"{self.middleman_url}/heartbeat",
                json={
                    'host_id': self.host_id,
                    'cpu_used': self.cpu_used,
                    'memory_used': self.memory_used,
                    'tasks_completed': self.tasks_completed,
                    'tasks_failed': self.tasks_failed
                },
                timeout=5
            )

            return response.status_code == 200

        except Exception as e:
            logger.warning(f"Heartbeat fehlgeschlagen: {e}")
            return False

    def simulate_availability(self):
        if not self.failure_simulation:
            return

        if random.random() > self.uptime_probability:
            if self.is_online:
                logger.warning(f"Host geht unerwartet offline")
                self.is_online = False
                time.sleep(random.randint(10, 60))  # 10-60s Ausfall
        else:
            if not self.is_online:
                logger.info(f"Host kommt wieder online")
                self.is_online = True
                self.register()  # Neu registrieren nach Ausfall eventuell das ändern weil ausfall und Ping gerade ähnlich wirken

    def execute_shard(self, shard):
        shard_id = shard['shard_id']
        duration = shard['duration']
        cpu_req = shard['cpu_required']
        mem_req = shard['memory_required']

        logger.info(f"Starte Shard {shard_id} (Teil {shard['shard_index'] + 1}/{shard['total_shards']}): "
                    f"{cpu_req:.2f} CPU, {mem_req:.2f} MB, {duration:.1f}s")

        self.cpu_used += cpu_req
        self.memory_used += mem_req

        try:
            time.sleep(duration)

            if random.random() < 0.03:
                raise Exception("Simulierter Shard-Fehler")

            logger.info(f"Shard {shard_id} erfolgreich abgeschlossen")
            self.tasks_completed += 1
            self.notify_shard_completion(shard_id, True)

        except Exception as e:
            logger.error(f"Shard {shard_id} fehlgeschlagen: {e}")
            self.tasks_failed += 1
            self.notify_shard_completion(shard_id, False)

        finally:
            self.cpu_used -= cpu_req
            self.memory_used -= mem_req

            if shard_id in self.active_shards:
                del self.active_shards[shard_id]

    def notify_shard_completion(self, shard_id, success):
        try:
            requests.post(
                f"{self.middleman_url}/shard/{shard_id}/complete",
                json={'success': success},
                timeout=5
            )
        except Exception as e:
            logger.error(f"Completion-Benachrichtigung fehlgeschlagen: {e}")

    def get_assigned_shards(self):
        try:
            response = requests.get(
                f"{self.middleman_url}/shards",
                timeout=5
            )

            if response.status_code == 200:
                all_shards = response.json()
                # Filtere Shards die diesem Host zugewiesen sind
                my_shards = [
                    s for s in all_shards
                    if s['assigned_to'] == self.host_id and s['status'] == 'assigned'
                ]
                return my_shards

        except Exception as e:
            logger.error(f"Fehler beim Abrufen von Shards: {e}")

        return []

    def can_execute_shard(self, shard):
        cpu_available = self.cpu_capacity - self.cpu_used
        mem_available = self.memory_capacity - self.memory_used

        return (shard['cpu_required'] <= cpu_available and
                shard['memory_required'] <= mem_available)

    def heartbeat_loop(self):
        while True:
            self.simulate_availability()

            if self.is_online:
                self.send_heartbeat()

            time.sleep(10)  

    def shard_execution_loop(self):

        while True:
            if self.is_online:
                shards = self.get_assigned_shards()

                for shard in shards:
                    shard_id = shard['shard_id']

                    # Prüfe ob shard schon läuft
                    if shard_id in self.active_shards:
                        continue

                    # Prüfe ob genug platz
                    if self.can_execute_shard(shard):
                        # Shard in separatem Thread ausführen
                        thread = threading.Thread(
                            target=self.execute_shard,
                            args=(shard,)
                        )
                        thread.start()
                        self.active_shards[shard_id] = thread
                    else:
                        logger.debug(f"Nicht genug Ressourcen für Shard {shard_id}")

            time.sleep(3)

    def run(self):

        while not self.register():
            logger.warning("Warte auf Middleman...")
            time.sleep(5)

        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.shard_execution_loop, daemon=True).start()

        logger.info(f"Host {self.host_id} läuft")

        while True:
            time.sleep(60)
            logger.info(f"Status: {'Online' if self.is_online else 'Offline'}, "
                        f"CPU: {self.cpu_used:.2f}/{self.cpu_capacity}, "
                        f"RAM: {self.memory_used:.0f}/{self.memory_capacity} MB, "
                        f"Aktive Shards: {len(self.active_shards)}, "
                        f"Completed: {self.tasks_completed}, Failed: {self.tasks_failed}")


if __name__ == '__main__':
    host = ProviderHost()
    host.run()