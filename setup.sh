#!/bin/bash

echo "================================================"
echo "Cloud-Ressourcen-Vermittlungssystem Setup"
echo "================================================"
echo ""

# Farben für Output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Prüfe Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker ist nicht installiert!${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ Docker Compose ist nicht installiert!${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker gefunden${NC}"
echo -e "${GREEN}✓ Docker Compose gefunden${NC}"
echo ""

# Erstelle Verzeichnisstruktur
echo "Erstelle Projektstruktur..."
mkdir -p middleman/logs
mkdir -p host/logs
mkdir -p client/logs
mkdir -p monitoring/grafana/{dashboards,datasources}
mkdir -p experiments
mkdir -p data

# Erstelle requirements.txt für middleman
cat > middleman/requirements.txt << EOF
flask==3.0.0
redis==5.0.1
requests==2.31.0
prometheus-client==0.19.0
pyyaml==6.0.1
schedule==1.2.0
EOF

# Erstelle requirements.txt für host
cat > host/requirements.txt << EOF
requests==2.31.0
EOF

# Erstelle requirements.txt für client
cat > client/requirements.txt << EOF
requests==2.31.0
EOF

# Erstelle Prometheus Datasource für Grafana
cat > monitoring/grafana/datasources/prometheus.yml << EOF
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
EOF

# Erstelle Grafana Dashboard
cat > monitoring/grafana/dashboards/dashboard.yml << EOF
apiVersion: 1

providers:
  - name: 'Default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /etc/grafana/provisioning/dashboards
EOF

# Erstelle .gitignore
cat > .gitignore << EOF
*.log
*.pyc
__pycache__/
*.swp
*.swo
.DS_Store
data/
*.csv
experiments/*.csv
EOF

echo -e "${GREEN} Projektstruktur erstellt${NC}"
echo ""

# Build Images
echo "Building Docker Images..."

docker-compose build --no-cache

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Images gebaut${NC}"
else
    echo -e "${RED} Fehler beim Image bauen${NC}"
    exit 1
fi

