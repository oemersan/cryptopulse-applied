#!/usr/bin/env bash
# =====================================================
# CryptoPulse - Health Check
# Quickly reports whether each service is up and reachable.
#
# Usage:
#   ./scripts/health_check.sh
# =====================================================

set -u

# Colors (no-op on Windows Git Bash without TTY)
if [ -t 1 ]; then
    GREEN='\033[0;32m'
    RED='\033[0;31m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    GREEN=''; RED=''; YELLOW=''; BLUE=''; BOLD=''; NC=''
fi

# Load .env if it exists, so we use the right ports.
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    . .env
    set +a
fi

# Sensible defaults if .env is missing values.
: "${POSTGRES_PORT:=5433}"
: "${PGADMIN_PORT:=5050}"
: "${AIRFLOW_WEBSERVER_PORT:=8088}"
: "${ELASTICSEARCH_PORT:=9200}"
: "${KIBANA_PORT:=5601}"

PASS=0
FAIL=0

print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}=== $1 ===${NC}"
}

# Check that a container is up. $1 = container name, $2 = label.
check_container() {
    local name=$1
    local label=$2
    if docker ps --format '{{.Names}}' | grep -q "^${name}$"; then
        local status
        status=$(docker inspect --format='{{.State.Status}}' "$name" 2>/dev/null || echo "unknown")
        local health
        health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' "$name" 2>/dev/null || echo "n/a")
        if [ "$status" = "running" ] && { [ "$health" = "healthy" ] || [ "$health" = "n/a" ]; }; then
            echo -e "  ${GREEN}OK${NC}    ${label} (${status}, health=${health})"
            PASS=$((PASS+1))
            return 0
        else
            echo -e "  ${RED}FAIL${NC}  ${label} (${status}, health=${health})"
            FAIL=$((FAIL+1))
            return 1
        fi
    else
        echo -e "  ${RED}FAIL${NC}  ${label} (container not running)"
        FAIL=$((FAIL+1))
        return 1
    fi
}

# Check that an HTTP endpoint returns 2xx/3xx. $1 = url, $2 = label.
check_http() {
    local url=$1
    local label=$2
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$url" 2>/dev/null || echo "000")
    if [[ "$code" =~ ^[23] ]]; then
        echo -e "  ${GREEN}OK${NC}    ${label} (HTTP ${code}) — ${url}"
        PASS=$((PASS+1))
        return 0
    else
        echo -e "  ${RED}FAIL${NC}  ${label} (HTTP ${code}) — ${url}"
        FAIL=$((FAIL+1))
        return 1
    fi
}

# Check Postgres row counts. $1 = table.
check_pg_count() {
    local table=$1
    local count
    count=$(docker exec cryptopulse-postgres \
        psql -U "${POSTGRES_USER:-cryptopulse}" -d "${POSTGRES_DB:-cryptopulse}" \
        -tAc "SELECT COUNT(*) FROM ${table};" 2>/dev/null || echo "ERR")
    if [ "$count" = "ERR" ] || [ -z "$count" ]; then
        echo -e "  ${RED}FAIL${NC}  postgres.${table} (could not query)"
        FAIL=$((FAIL+1))
    elif [ "$count" -eq 0 ]; then
        echo -e "  ${YELLOW}WARN${NC}  postgres.${table} = 0 rows (no data ingested yet?)"
    else
        echo -e "  ${GREEN}OK${NC}    postgres.${table} = ${count} rows"
        PASS=$((PASS+1))
    fi
}

# Check Elasticsearch index doc counts. $1 = index name.
check_es_count() {
    local index=$1
    local response
    response=$(curl -s --max-time 5 "http://localhost:${ELASTICSEARCH_PORT}/${index}/_count" 2>/dev/null || echo "")
    if [ -z "$response" ]; then
        echo -e "  ${RED}FAIL${NC}  es[${index}] (no response)"
        FAIL=$((FAIL+1))
        return
    fi
    # Extract the integer after "count":
    local count
    count=$(echo "$response" | grep -o '"count":[0-9]*' | head -1 | cut -d: -f2)
    if [ -z "$count" ]; then
        echo -e "  ${YELLOW}WARN${NC}  es[${index}] (index may not exist yet)"
    elif [ "$count" -eq 0 ]; then
        echo -e "  ${YELLOW}WARN${NC}  es[${index}] = 0 docs"
    else
        echo -e "  ${GREEN}OK${NC}    es[${index}] = ${count} docs"
        PASS=$((PASS+1))
    fi
}


echo -e "${BOLD}CryptoPulse Health Check${NC}"
echo "$(date)"

print_header "Containers"
check_container "cryptopulse-postgres"           "postgres"
check_container "cryptopulse-pgadmin"            "pgadmin"
check_container "cryptopulse-airflow-webserver"  "airflow-webserver"
check_container "cryptopulse-airflow-scheduler"  "airflow-scheduler"
check_container "cryptopulse-elasticsearch"      "elasticsearch"
check_container "cryptopulse-kibana"             "kibana"
check_container "cryptopulse-pipeline"           "pipeline"

print_header "HTTP endpoints"
check_http "http://localhost:${AIRFLOW_WEBSERVER_PORT}/health"          "Airflow"
check_http "http://localhost:${ELASTICSEARCH_PORT}/_cluster/health"     "Elasticsearch"
check_http "http://localhost:${KIBANA_PORT}/api/status"                 "Kibana"
check_http "http://localhost:${PGADMIN_PORT}/"                          "pgAdmin"

print_header "Data — Postgres"
check_pg_count "prices"
check_pg_count "news"
check_pg_count "anomalies"
check_pg_count "anomaly_news_links"

print_header "Data — Elasticsearch"
check_es_count "prices"
check_es_count "news"
check_es_count "anomalies"
check_es_count "anomaly_news_context"

echo ""
echo -e "${BOLD}Summary:${NC} ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo ""

if [ $FAIL -gt 0 ]; then
    echo "Tip: inspect a failing service with"
    echo "    docker compose logs <service-name>"
    exit 1
fi
exit 0
