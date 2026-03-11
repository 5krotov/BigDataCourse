#!/usr/bin/env bash
# =============================================================================
# setup_hive4.sh — полная установка и настройка Hive 4.0.0-alpha-2
# Кластер: Hadoop 3.4.3, Derby metastore, local MR mode
# =============================================================================
set -euo pipefail

HADOOP_HOME="/home/hadoop/hadoop-3.4.3"
HIVE_HOME="/home/hadoop/apache-hive-4.0.0-alpha-2-bin"
HIVE_ARCHIVE="apache-hive-4.0.0-alpha-2-bin.tar.gz"
HIVE_URL="https://archive.apache.org/dist/hive/hive-4.0.0-alpha-2/$HIVE_ARCHIVE"
METASTORE_DB="/home/hadoop/hive4_metastore_db"
LOG_DIR="/tmp/hadoop"
HDFS="$HADOOP_HOME/bin/hdfs"

export PATH="$HIVE_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
log()  { echo "[$(date '+%H:%M:%S')] → $*"; }
err()  { echo "[$(date '+%H:%M:%S')] ✗ $*" >&2; exit 1; }
phase() { echo ""; echo "══════════════════════════════════════════════════"; \
          echo "  $*"; \
          echo "══════════════════════════════════════════════════"; }

# ─────────────────────────────────────────────────────────────────────────────
phase "[1/9] Остановка Hive-процессов"
# ─────────────────────────────────────────────────────────────────────────────
PIDS=$(jps 2>/dev/null | grep RunJar | awk '{print $1}') || true
if [[ -n "$PIDS" ]]; then
    echo "$PIDS" | xargs kill 2>/dev/null || true
    sleep 5
    ok "Hive-процессы остановлены"
else
    log "Hive не был запущен"
fi

# ─────────────────────────────────────────────────────────────────────────────
phase "[2/9] Загрузка и распаковка Hive 4.0.0-alpha-2"
# ─────────────────────────────────────────────────────────────────────────────
cd /home/hadoop

if [[ ! -d "$HIVE_HOME" ]]; then
    if [[ ! -f "$HIVE_ARCHIVE" ]]; then
        log "Скачиваем $HIVE_URL ..."
        wget -q --show-progress "$HIVE_URL"
    else
        log "Архив уже есть, пропускаем загрузку"
    fi
    gzip -t "$HIVE_ARCHIVE" || err "Архив повреждён — удали $HIVE_ARCHIVE и запусти снова"
    tar -xzf "$HIVE_ARCHIVE"
    ok "Распакован в $HIVE_HOME"
else
    log "$HIVE_HOME уже существует, пропускаем"
fi

# ─────────────────────────────────────────────────────────────────────────────
phase "[3/9] Конфигурация hive-site.xml"
# ─────────────────────────────────────────────────────────────────────────────
mkdir -p "$HIVE_HOME/conf"
[[ -f "$HIVE_HOME/conf/hive-site.xml" ]] && \
    cp "$HIVE_HOME/conf/hive-site.xml" "$HIVE_HOME/conf/hive-site.xml.bak"

cat > "$HIVE_HOME/conf/hive-site.xml" << 'XML'
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<configuration>

  <!-- Metastore -->
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://localhost:9083</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>

  <!-- Derby (embedded) -->
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/home/hadoop/hive4_metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>

  <!-- HiveServer2 -->
  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>
  <property>
    <name>hive.server2.webui.port</name>
    <value>10002</value>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.security.authorization.enabled</name>
    <value>false</value>
  </property>

  <!-- Динамическое партиционирование -->
  <property>
    <name>hive.exec.dynamic.partition</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.exec.dynamic.partition.mode</name>
    <value>nonstrict</value>
  </property>
  <property>
    <name>hive.exec.max.dynamic.partitions</name>
    <value>2000</value>
  </property>

  <!-- FIX: tasks выполняются в JVM HiveServer2, не в child-процессе →
       commons-collections и другие Hive-jar видны в classloader -->
  <property>
    <name>hive.exec.submit.local.task.via.child</name>
    <value>false</value>
  </property>

  <!-- FIX: единый classloader для всех MR job в сессии -->
  <property>
    <name>mapreduce.job.classloader</name>
    <value>false</value>
  </property>

  <!-- FIX: отключаем autogather — лишние MR job -->
  <property>
    <name>hive.stats.autogather</name>
    <value>false</value>
  </property>

  <!-- Логи -->
  <property>
    <name>hive.log.dir</name>
    <value>/tmp/hadoop</value>
  </property>
  <property>
    <name>hive.log.file</name>
    <value>hive.log</value>
  </property>

</configuration>
XML
ok "hive-site.xml записан"

# ─────────────────────────────────────────────────────────────────────────────
phase "[4/9] Патч mapred-site.xml"
# ─────────────────────────────────────────────────────────────────────────────
python3 - << PYEOF
import re

conf = "$HADOOP_HOME/etc/hadoop/mapred-site.xml"
with open(conf) as f:
    content = f.read()

patches = {
    # Локальный MR без YARN
    "mapreduce.framework.name": "local",
    # FIX OOM: MapOutputBuffer выделяет этот буфер в heap HiveServer2
    # дефолт 100 MB вызывает OOM, 32 MB достаточно для любого датасета
    "mapreduce.task.io.sort.mb": "32",
    # FIX ClassNotFoundException: hive/lib/* попадает в classloader каждого MR task
    "mapreduce.application.classpath": (
        "\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:"
        "\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:"
        "\$HADOOP_HOME/share/hadoop/common/*:"
        "\$HADOOP_HOME/share/hadoop/common/lib/*:"
        "$HIVE_HOME/lib/*"
    ),
}

for key, value in patches.items():
    prop = f"\n  <property><name>{key}</name><value>{value}</value></property>"
    if key not in content:
        content = content.replace("</configuration>", prop + "\n</configuration>", 1)
        print(f"  ADD: {key}")
    else:
        content = re.sub(
            rf"(<name>{re.escape(key)}</name>\s*<value>)[^<]*(</value>)",
            rf"\g<1>{re.escape(value)}\g<2>",
            content,
        )
        print(f"  UPD: {key}")

with open(conf, "w") as f:
    f.write(content)
print("  mapred-site.xml OK")
PYEOF
ok "mapred-site.xml обновлён"

# ─────────────────────────────────────────────────────────────────────────────
phase "[5/9] Патч core-site.xml (proxyuser)"
# ─────────────────────────────────────────────────────────────────────────────
python3 - << PYEOF
conf = "$HADOOP_HOME/etc/hadoop/core-site.xml"
with open(conf) as f:
    content = f.read()

if "proxyuser.hadoop.hosts" not in content:
    inject = """
  <property>
    <name>hadoop.proxyuser.hadoop.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.hadoop.groups</name>
    <value>*</value>
  </property>"""
    content = content.replace("</configuration>", inject + "\n</configuration>", 1)
    with open(conf, "w") as f:
        f.write(content)
    print("  proxyuser добавлен")
else:
    print("  proxyuser уже есть")
PYEOF

# Применяем без рестарта NameNode
$HDFS dfsadmin -refreshSuperUserGroupsConfiguration 2>/dev/null && ok "proxyuser применён" || true

# ─────────────────────────────────────────────────────────────────────────────
phase "[6/9] Симлинк commons-collections в mapreduce/lib"
# ─────────────────────────────────────────────────────────────────────────────
# FIX: LocalJobRunner строит classloader task'а из mapreduce/lib/*
# commons-collections-3.x нужен для GROUP BY / ORDER BY операторов
CC_JAR=$(find "$HIVE_HOME/lib" -name "commons-collections-3*.jar" | head -1)
[[ -n "$CC_JAR" ]] || err "commons-collections-3.x не найден в $HIVE_HOME/lib"
ln -sf "$CC_JAR" "$HADOOP_HOME/share/hadoop/mapreduce/lib/$(basename $CC_JAR)"
ok "Симлинк: $(basename $CC_JAR) → mapreduce/lib/"

# ─────────────────────────────────────────────────────────────────────────────
phase "[7/9] Директории HDFS"
# ─────────────────────────────────────────────────────────────────────────────
jps 2>/dev/null | grep -q "NameNode" || err "NameNode не запущен — запусти HDFS: $HADOOP_HOME/sbin/start-dfs.sh"

for dir in /user/hive/warehouse /user/hive/staging /tmp/hive /data/employees_raw; do
    $HDFS dfs -mkdir -p "$dir" 2>/dev/null && log "mkdir $dir" || log "$dir уже существует"
done

$HDFS dfs -chmod 777 /tmp
$HDFS dfs -chmod 777 /tmp/hive
$HDFS dfs -chmod 777 /user/hive/warehouse
ok "Права HDFS выставлены"

# ─────────────────────────────────────────────────────────────────────────────
phase "[8/9] Инициализация схемы Derby Metastore"
# ─────────────────────────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
if [[ -d "$METASTORE_DB" ]]; then
    log "Derby metastore уже существует ($METASTORE_DB), пропускаем"
else
    "$HIVE_HOME/bin/schematool" -dbType derby -initSchema 2>/dev/null \
        && ok "Схема Derby создана" \
        || err "Ошибка initSchema — см. лог выше"
fi

# ─────────────────────────────────────────────────────────────────────────────
phase "[9/9] Создание ~/start-hive.sh"
# ─────────────────────────────────────────────────────────────────────────────
cat > /home/hadoop/start-hive.sh << 'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail

HADOOP_HOME="/home/hadoop/hadoop-3.4.3"
HIVE_HOME="/home/hadoop/apache-hive-4.0.0-alpha-2-bin"
LOG_DIR="/tmp/hadoop"
METASTORE_PORT=9083
HS2_PORT=10000

export PATH="$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
err()  { echo "[$(date '+%H:%M:%S')] ✗ $*" >&2; exit 1; }

wait_port() {
    local name=$1 port=$2 retries=${3:-30} interval=${4:-10} pid=${5:-0}
    echo -n "[$(date '+%H:%M:%S')] Waiting $name "
    for i in $(seq 1 "$retries"); do
        nc -z localhost "$port" 2>/dev/null && { echo "→ UP"; return 0; }
        if [[ "$pid" -gt 0 ]] && ! kill -0 "$pid" 2>/dev/null; then
            echo ""
            err "$name упал! Лог: $LOG_DIR/hive-${name}.log"
        fi
        echo -n "."
        sleep "$interval"
    done
    echo ""
    err "$name не запустился за $((retries * interval))s"
}

cmd_stop() {
    local pids
    pids=$(jps 2>/dev/null | grep RunJar | awk '{print $1}') || true
    if [[ -n "$pids" ]]; then
        log "Останавливаем Hive (pids: $pids)..."
        echo "$pids" | xargs kill 2>/dev/null || true
        sleep 5
        ok "Hive остановлен"
    else
        log "Hive не запущен"
    fi
}

cmd_start() {
    jps 2>/dev/null | grep -q "NameNode" || \
        err "NameNode не запущен. Сначала: $HADOOP_HOME/sbin/start-dfs.sh"
    mkdir -p "$LOG_DIR"

    if nc -z localhost "$METASTORE_PORT" 2>/dev/null; then
        ok "Metastore уже запущен [:$METASTORE_PORT]"
    else
        log "Запуск Metastore..."
        nohup hive --service metastore >> "$LOG_DIR/hive-metastore.log" 2>&1 &
        local mpid=$!
        echo $mpid > /tmp/hive-metastore.pid
        wait_port "metastore" "$METASTORE_PORT" 12 5 "$mpid"
        ok "Metastore запущен [pid=$mpid]"
    fi

    if nc -z localhost "$HS2_PORT" 2>/dev/null; then
        ok "HiveServer2 уже запущен [:$HS2_PORT]"
    else
        log "Запуск HiveServer2..."
        nohup hive --service hiveserver2 >> "$LOG_DIR/hive-server2.log" 2>&1 &
        local hpid=$!
        echo $hpid > /tmp/hive-server2.pid
        wait_port "hiveserver2" "$HS2_PORT" 30 10 "$hpid"
        ok "HiveServer2 запущен [pid=$hpid]"
    fi

    echo ""
    ok "Hive готов → jdbc:hive2://localhost:$HS2_PORT"
    echo "   beeline -u \"jdbc:hive2://localhost:$HS2_PORT\""
}

cmd_status() {
    echo "── JVM процессы ──────────────────────────────"
    jps 2>/dev/null | grep -E "RunJar|NameNode|DataNode" || echo "  (нет)"
    echo ""
    echo "── Порты ─────────────────────────────────────"
    nc -z localhost "$METASTORE_PORT" 2>/dev/null \
        && ok "Metastore   [:$METASTORE_PORT]  UP" \
        || log "Metastore   [:$METASTORE_PORT]  DOWN"
    nc -z localhost "$HS2_PORT" 2>/dev/null \
        && ok "HiveServer2 [:$HS2_PORT] UP" \
        || log "HiveServer2 [:$HS2_PORT] DOWN"
}

case "${1:-start}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_stop; cmd_start ;;
    status)  cmd_status ;;
    logs)
        case "${2:-hs2}" in
            metastore|ms) tail -f "$LOG_DIR/hive-metastore.log" ;;
            hs2|*)        tail -f "$LOG_DIR/hive-server2.log" ;;
        esac ;;
    *)
        echo "Использование: $0 {start|stop|restart|status|logs [metastore|hs2]}"
        exit 1 ;;
esac
SCRIPT

chmod +x /home/hadoop/start-hive.sh
ok "start-hive.sh создан"

# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════"
ok "setup_hive4.sh завершён"
echo ""
echo "  Запустить Hive:   ~/start-hive.sh start"
echo "  Остановить:       ~/start-hive.sh stop"
echo "  Статус:           ~/start-hive.sh status"
echo "  Логи HS2:         ~/start-hive.sh logs hs2"
echo ""
echo "  Подключение:"
echo "  beeline -u \"jdbc:hive2://localhost:10000\""
echo "════════════════════════════════════════════════════"
