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
        
        logger.info(f"Host initialisiert: {self.host_id} ({self.name})")
        logger.info(f"Kapazität: {self.cpu_capacity} CPU, {self.memory_capacity} MB RAM")
        logger.info(f"Uptime-Wahrscheinlichkeit: {self.uptime_probability*100}%")
    
    def register(self):
        """Registriert den Host beim Middleman"""
        try:
            response = requests.post(
                f"{self.middleman_url}/register",
                json={
                    'host_id': self.host_id,
                    'name': self.name,
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
        """Sendet Heartbeat an Middleman"""
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
        """Simuliert wechselnde Verfügbarkeit"""
        if not self.failure_simulation:
            return
        
        # Mit bestimmter Wahrscheinlichkeit offline gehen
        if random.random() > self.uptime_probability:
            if self.is_online:
                logger.warning(f"Host geht offline (simulierter Ausfall)")
                self.is_online = False
                time.sleep(random.randint(10, 60))  # 10-60s Ausfall
        else:
            if not self.is_online:
                logger.info(f"Host kommt wieder online")
                self.is_online = True
                self.register()  # Neu registrieren nach Ausfall
    
    def execute_task(self, task):
        """Simuliert Ausführung einer Aufgabe"""
        task_id = task['task_id']
        duration = task['duration']
        cpu_req = task['cpu_required']
        mem_req = task['memory_required']
        
        logger.info(f"Starte Aufgabe {task_id}: {cpu_req} CPU, {mem_req} MB, {duration}s")
        
        # Ressourcen belegen
        self.cpu_used += cpu_req
        self.memory_used += mem_req
        
        try:
            # Simuliere Arbeit
            time.sleep(duration)
            
            # Zufällige Fehler (5% Chance)
            if random.random() < 0.05:
                raise Exception("Simulierter Task-Fehler")
            
            # Erfolg
            logger.info(f"Aufgabe {task_id} erfolgreich abgeschlossen")
            self.tasks_completed += 1
            self.notify_completion(task_id, True)
            
        except Exception as e:
            logger.error(f"Aufgabe {task_id} fehlgeschlagen: {e}")
            self.tasks_failed += 1
            self.notify_completion(task_id, False)
            
        finally:
            # Ressourcen freigeben
            self.cpu_used -= cpu_req
            self.memory_used -= mem_req
    
    def notify_completion(self, task_id, success):
        """Benachrichtigt Middleman über Task-Completion"""
        try:
            requests.post(
                f"{self.middleman_url}/task/{task_id}/complete",
                json={'success': success},
                timeout=5
            )
        except Exception as e:
            logger.error(f"Completion-Benachrichtigung fehlgeschlagen: {e}")
    
    def get_assigned_tasks(self):
        "Holt zugewiesene Aufgaben vom Middleman"
        try:
            response = requests.get(
                f"{self.middleman_url}/tasks",
                timeout=5
            )
            
            if response.status_code == 200:
                all_tasks = response.json()
                # Filtere Tasks die diesem Host zugewiesen sind
                my_tasks = [
                    t for t in all_tasks 
                    if t['assigned_to'] == self.host_id and t['status'] == 'assigned'
                ]
                return my_tasks
            
        except Exception as e:
            logger.error(f"Fehler beim Abrufen von Tasks: {e}")
        
        return []
    
    def heartbeat_loop(self):
        "Hintergrundloop für rückgabe von heartbeat"
        while True:
            self.simulate_availability()
            
            if self.is_online:
                self.send_heartbeat()
            
            time.sleep(10)  # Alle 10 Sekunden
    
    def task_execution_loop(self):
        "Hintergrundloop für tasks-ausführung"
        while True:
            if self.is_online:
                tasks = self.get_assigned_tasks()
                
                for task in tasks:
                    # Prüfe ob genug Ressourcen frei sind
                    cpu_available = self.cpu_capacity - self.cpu_used
                    mem_available = self.memory_capacity - self.memory_used
                    
                    if (task['cpu_required'] <= cpu_available and 
                        task['memory_required'] <= mem_available):
                        
                        # Task in separatem Thread ausführen
                        thread = threading.Thread(
                            target=self.execute_task,
                            args=(task,)
                        )
                        thread.start()
            
            time.sleep(5)
    
    def run(self):
        "Startet den Host"
        # Initial registrieren
        while not self.register():
            logger.warning("Warte auf Middleman...")
            time.sleep(5)
        
        # Background-Threads hochfahren
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.task_execution_loop, daemon=True).start()
        
        logger.info(f"Host {self.host_id} läuft")
        
        
        while True:
            time.sleep(60)
            logger.info(f"Status: {'Online' if self.is_online else 'Offline'}, "
                       f"CPU: {self.cpu_used}/{self.cpu_capacity}, "
                       f"RAM: {self.memory_used}/{self.memory_capacity} MB, "
                       f"Completed: {self.tasks_completed}, Failed: {self.tasks_failed}")

if __name__ == '__main__':
    host = ProviderHost()
    host.run()