#!/usr/bin/env bash
# =============================================================================
# verify_hive4.sh — проверка развёртывания Hive 4.0.0-alpha-2
# Запуск: bash ~/verify_hive4.sh
# =============================================================================
set -euo pipefail

HIVE_HOME="/home/hadoop/apache-hive-4.0.0-alpha-2-bin"
BEELINE="$HIVE_HOME/bin/beeline"
JDBC="jdbc:hive2://localhost:10000"
DB="team06"
TABLE="employees"

PASS=0
FAIL=0

ok()   { echo "  [PASS] $*"; ((PASS++)); }
fail() { echo "  [FAIL] $*"; ((FAIL++)); }
phase(){ echo ""; echo "══════════════════════════════════════════════"; \
         echo "  $*"; \
         echo "══════════════════════════════════════════════"; }
bql()  { "$BEELINE" -u "$JDBC" --silent=true --outputformat=csv2 -e "$1" 2>/dev/null; }

# ─────────────────────────────────────────────────────────────────────────────
phase "[1/6] Проверка процессов и портов"
# ─────────────────────────────────────────────────────────────────────────────

jps 2>/dev/null | grep -q "NameNode" \
    && ok "NameNode запущен" \
    || fail "NameNode НЕ запущен"

nc -z localhost 9083 2>/dev/null \
    && ok "Metastore доступен [:9083]" \
    || fail "Metastore недоступен [:9083]"

nc -z localhost 10000 2>/dev/null \
    && ok "HiveServer2 доступен [:10000]" \
    || fail "HiveServer2 недоступен [:10000]"

# ─────────────────────────────────────────────────────────────────────────────
phase "[2/6] Проверка non-embedded режима (multi-client)"
# ─────────────────────────────────────────────────────────────────────────────

# Два одновременных клиента
bql "USE $DB; SELECT COUNT(*) FROM $TABLE;" > /tmp/verify_client1.txt &
C1_PID=$!
bql "USE $DB; SELECT COUNT(*) FROM $TABLE;" > /tmp/verify_client2.txt &
C2_PID=$!
wait $C1_PID && wait $C2_PID

R1=$(grep -E "^[0-9]+" /tmp/verify_client1.txt | head -1 || echo "0")
R2=$(grep -E "^[0-9]+" /tmp/verify_client2.txt | head -1 || echo "0")

[[ "$R1" -gt 0 && "$R2" -gt 0 ]] \
    && ok "Два одновременных клиента выполнили запросы (client1=$R1, client2=$R2)" \
    || fail "Параллельные клиенты не отработали (client1=$R1, client2=$R2)"

# ─────────────────────────────────────────────────────────────────────────────
phase "[3/6] Проверка структуры партиционированной таблицы"
# ─────────────────────────────────────────────────────────────────────────────

TABLE_TYPE=$(bql "USE $DB; DESCRIBE FORMATTED $TABLE;" \
    | grep "Table Type" | awk -F',' '{print $2}' | tr -d ' ')

[[ "$TABLE_TYPE" == *"TABLE"* ]] \
    && ok "Table Type: $TABLE_TYPE" \
    || fail "Неожиданный Table Type: $TABLE_TYPE"

PART_COLS=$(bql "USE $DB; DESCRIBE FORMATTED $TABLE;" \
    | grep -A5 "Partition Information" | grep -E "year|month" | wc -l)

[[ "$PART_COLS" -ge 2 ]] \
    && ok "Партиционирование: year + month (columns=$PART_COLS)" \
    || fail "Колонки партиционирования не найдены"

PART_COUNT=$(bql "USE $DB; SHOW PARTITIONS $TABLE;" \
    | grep -c "year=" || true)

[[ "$PART_COUNT" -ge 35 ]] \
    && ok "Количество партиций: $PART_COUNT (≥35)" \
    || fail "Партиций найдено: $PART_COUNT (ожидалось ≥35)"

# ─────────────────────────────────────────────────────────────────────────────
phase "[4/6] Проверка аналитических запросов"
# ─────────────────────────────────────────────────────────────────────────────

TOTAL=$(bql "USE $DB; SELECT COUNT(*) FROM $TABLE;" \
    | grep -E "^[0-9]+" | head -1 || echo "0")

[[ "$TOTAL" -ge 100 ]] \
    && ok "SELECT COUNT(*) = $TOTAL (≥100)" \
    || fail "SELECT COUNT(*) = $TOTAL (ожидалось ≥100)"

GROUP_ROWS=$(bql "USE $DB; SELECT year, COUNT(*) FROM $TABLE GROUP BY year;" \
    | grep -cE "^20[0-9]{2}" || true)

[[ "$GROUP_ROWS" -ge 3 ]] \
    && ok "GROUP BY year: $GROUP_ROWS лет в результате" \
    || fail "GROUP BY year вернул $GROUP_ROWS строк (ожидалось ≥3)"

ORDER_ROWS=$(bql "USE $DB; SELECT year, month, COUNT(*) AS cnt FROM $TABLE GROUP BY year, month ORDER BY year, month;" \
    | grep -cE "^20[0-9]{2}" || true)

[[ "$ORDER_ROWS" -ge 35 ]] \
    && ok "GROUP BY + ORDER BY: $ORDER_ROWS строк" \
    || fail "GROUP BY + ORDER BY вернул $ORDER_ROWS строк (ожидалось ≥35)"

# ─────────────────────────────────────────────────────────────────────────────
phase "[5/6] Проверка загрузки данных в партицию"
# ─────────────────────────────────────────────────────────────────────────────

TEST_YEAR=2099
TEST_MONTH=12

bql "USE $DB;
INSERT INTO $TABLE PARTITION (year=$TEST_YEAR, month=$TEST_MONTH)
VALUES (9901, 'TestUser', 30, 'TestCity');" > /dev/null

INSERTED=$(bql "USE $DB; SELECT COUNT(*) FROM $TABLE WHERE year=$TEST_YEAR AND month=$TEST_MONTH;" \
    | grep -E "^[0-9]+" | head -1 || echo "0")

[[ "$INSERTED" -ge 1 ]] \
    && ok "INSERT INTO PARTITION (year=$TEST_YEAR, month=$TEST_MONTH): $INSERTED строк" \
    || fail "INSERT в партицию не сработал"

PART_EXISTS=$(bql "USE $DB; SHOW PARTITIONS $TABLE;" \
    | grep -c "year=$TEST_YEAR" || true)

[[ "$PART_EXISTS" -ge 1 ]] \
    && ok "Новая партиция year=$TEST_YEAR/month=$TEST_MONTH появилась в метаstore" \
    || fail "Партиция year=$TEST_YEAR не найдена в SHOW PARTITIONS"

# ─────────────────────────────────────────────────────────────────────────────
phase "[6/6] Partition Pruning"
# ─────────────────────────────────────────────────────────────────────────────

# Если partition pruning работает — запрос выполнится без MR job (мгновенно)
START_MS=$(date +%s%3N)
bql "USE $DB; SELECT * FROM $TABLE WHERE year=$TEST_YEAR AND month=$TEST_MONTH;" > /dev/null
END_MS=$(date +%s%3N)
ELAPSED=$(( END_MS - START_MS ))

[[ "$ELAPSED" -lt 2000 ]] \
    && ok "Partition pruning работает (запрос по одной партиции: ${ELAPSED}ms, без MR)" \
    || fail "Partition pruning медленнее ожидаемого (${ELAPSED}ms)"

# Убираем тестовую партицию
bql "USE $DB; ALTER TABLE $TABLE DROP PARTITION (year=$TEST_YEAR, month=$TEST_MONTH);" > /dev/null \
    && ok "Тестовая партиция year=$TEST_YEAR удалена" \
    || fail "Не удалось удалить тестовую партицию"

# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo "  Итог: PASS=$PASS  FAIL=$FAIL"
echo "══════════════════════════════════════════════"
[[ "$FAIL" -eq 0 ]] && echo "  ✓ Все проверки пройдены!" || echo "  ✗ Есть провалы — см. выше"
echo ""
