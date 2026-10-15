#!/bin/sh
# Respaldo de TimescaleDB con pg_dump (formato custom, comprimido). Corre DENTRO del contenedor
# db-backup; en Windows no hace falta ejecutarlo a mano (centinela.cmd respaldo lo invoca).
#   respaldo.sh ciclo  -> un respaldo al arrancar y luego cada BACKUP_INTERVAL_HOURS horas
#   respaldo.sh ahora  -> un solo respaldo (manual)
#   respaldo.sh restaurar <archivo.dump> -> reemplaza la base por ese respaldo (procedimiento de
#                         TimescaleDB: timescaledb_pre_restore / pg_restore / timescaledb_post_restore)
# Conserva los BACKUP_KEEP más recientes. Se escribe a un archivo temporal y se renombra al
# terminar, para que un respaldo cortado a la mitad nunca reemplace a uno bueno.
set -u
DIR=/backups
INTERVAL_HOURS="${BACKUP_INTERVAL_HOURS:-6}"
KEEP="${BACKUP_KEEP:-8}"

respaldar() {
  mkdir -p "$DIR"
  f="$DIR/centinela-$(date +%Y%m%d-%H%M%S).dump"
  if pg_dump --format=custom --file="$f.tmp" 2>"$DIR/.ultimo-error"; then
    mv "$f.tmp" "$f"
    rm -f "$DIR/.ultimo-error"
    echo "$(date -Iseconds) respaldo OK: $(basename "$f") ($(du -h "$f" | cut -f1))"
  else
    rm -f "$f.tmp"
    echo "$(date -Iseconds) respaldo FALLIDO: $(cat "$DIR/.ultimo-error")" >&2
    return 1
  fi
  # Retención: borra los más viejos por encima de KEEP
  ls -1t "$DIR"/centinela-*.dump 2>/dev/null | tail -n +"$((KEEP + 1))" | while read -r viejo; do
    rm -f "$viejo" && echo "$(date -Iseconds) respaldo antiguo eliminado: $(basename "$viejo")"
  done
}

restaurar() {
  archivo="$DIR/$(basename "$1")"
  [ -f "$archivo" ] || { echo "No existe $archivo" >&2; exit 1; }
  echo "$(date -Iseconds) restaurando $(basename "$archivo")..."
  # FORCE corta las conexiones abiertas (alert-service se reconecta solo al terminar)
  psql -v ON_ERROR_STOP=1 -q -d postgres -c "DROP DATABASE IF EXISTS $PGDATABASE WITH (FORCE)" \
    -c "CREATE DATABASE $PGDATABASE" || exit 1
  psql -v ON_ERROR_STOP=1 -q -d "$PGDATABASE" -c "CREATE EXTENSION IF NOT EXISTS timescaledb" \
    -c "SELECT timescaledb_pre_restore()" >/dev/null || exit 1
  pg_restore --no-owner -d "$PGDATABASE" "$archivo" 2>&1 | grep -v -i warning
  psql -v ON_ERROR_STOP=1 -q -d "$PGDATABASE" -c "SELECT timescaledb_post_restore()" >/dev/null || exit 1
  echo "$(date -Iseconds) restauración terminada: $(psql -At -d "$PGDATABASE" -c 'select count(*) from alerts') alertas, $(psql -At -d "$PGDATABASE" -c 'select count(*) from sensor_events') eventos"
}

case "${1:-ciclo}" in
  ahora) respaldar ;;
  restaurar) [ $# -ge 2 ] || { echo "uso: respaldo.sh restaurar <archivo.dump>" >&2; exit 2; }; restaurar "$2" ;;
  ciclo)
    until pg_isready -q; do sleep 5; done
    while true; do
      respaldar || true
      sleep "$((INTERVAL_HOURS * 3600))"
    done ;;
  *) echo "uso: respaldo.sh [ciclo|ahora|restaurar <archivo.dump>]" >&2; exit 2 ;;
esac
