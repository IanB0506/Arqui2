#!/bin/sh
# Respaldo de TimescaleDB con pg_dump (formato custom, comprimido). Corre DENTRO del contenedor
# db-backup; en Windows no hace falta ejecutarlo a mano (centinela.cmd respaldo lo invoca).
#   respaldo.sh ciclo  -> un respaldo cada BACKUP_INTERVAL_HOURS horas (al arrancar solo si no hay
#                         uno reciente, para no rotar respaldos buenos con cada reinicio)
#   respaldo.sh ahora  -> un solo respaldo (manual)
#   respaldo.sh restaurar <archivo.dump> -> reemplaza la base por ese respaldo. Se restaura en una
#                         base temporal a la que nadie se conecta (procedimiento de TimescaleDB:
#                         pre_restore / pg_restore / post_restore) y al final se intercambia por la
#                         actual: los servicios no escriben a medias sobre una base en restauración.
# Conserva los BACKUP_KEEP más recientes. Se escribe a un archivo temporal y se renombra al
# terminar, para que un respaldo cortado a la mitad nunca reemplace a uno bueno.
set -u
DIR=/backups
INTERVAL_HOURS="${BACKUP_INTERVAL_HOURS:-6}"
KEEP="${BACKUP_KEEP:-8}"

respaldar() {
  mkdir -p "$DIR"
  # Una base recien creada (sin tablas todavia) no se respalda: no debe desplazar a los buenos
  if [ "$(psql -At -c "select to_regclass('public.alerts') is not null" 2>/dev/null)" != "t" ]; then
    echo "$(date -Iseconds) base sin datos todavia: no se respalda"
    return 0
  fi
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
  tmp="${PGDATABASE}_restaurando"
  echo "$(date -Iseconds) restaurando $(basename "$archivo")..."
  psql -v ON_ERROR_STOP=1 -q -d postgres -c "DROP DATABASE IF EXISTS $tmp" -c "CREATE DATABASE $tmp" || exit 1
  psql -v ON_ERROR_STOP=1 -q -d "$tmp" -c "CREATE EXTENSION IF NOT EXISTS timescaledb" \
    -c "SELECT timescaledb_pre_restore()" >/dev/null || exit 1
  pg_restore --no-owner -d "$tmp" "$archivo" >"$DIR/.restauracion.log" 2>&1
  rc=$?
  grep -v -i warning "$DIR/.restauracion.log"
  psql -v ON_ERROR_STOP=1 -q -d "$tmp" -c "SELECT timescaledb_post_restore()" >/dev/null || exit 1
  if [ "$(psql -At -d "$tmp" -c "select to_regclass('public.alerts') is not null")" != "t" ]; then
    echo "La restauracion fallo (sin tablas); la base actual NO se toco. Detalle: $DIR/.restauracion.log" >&2
    exit 1
  fi
  [ "$rc" -eq 0 ] || echo "Aviso: pg_restore termino con codigo $rc (ver $DIR/.restauracion.log)"
  # Intercambio: FORCE corta las conexiones a la base actual; los servicios se reconectan solos
  psql -v ON_ERROR_STOP=1 -q -d postgres -c "DROP DATABASE IF EXISTS $PGDATABASE WITH (FORCE)" \
    -c "ALTER DATABASE $tmp RENAME TO $PGDATABASE" || exit 1
  rm -f "$DIR/.restauracion.log"
  echo "$(date -Iseconds) restauración terminada: $(psql -At -d "$PGDATABASE" -c 'select count(*) from alerts') alertas, $(psql -At -d "$PGDATABASE" -c 'select count(*) from sensor_events') eventos"
}

case "${1:-ciclo}" in
  ahora) respaldar ;;
  restaurar) [ $# -ge 2 ] || { echo "uso: respaldo.sh restaurar <archivo.dump>" >&2; exit 2; }; restaurar "$2" ;;
  ciclo)
    until pg_isready -q; do sleep 5; done
    # Si ya hay un respaldo reciente (p. ej. tras reiniciar la PC), se espera al siguiente turno
    if [ -n "$(find "$DIR" -name 'centinela-*.dump' -mmin -"$((INTERVAL_HOURS * 60))" 2>/dev/null | head -n 1)" ]; then
      echo "$(date -Iseconds) hay un respaldo de las ultimas $INTERVAL_HOURS h: el proximo sera en $INTERVAL_HOURS h"
      sleep "$((INTERVAL_HOURS * 3600))"
    fi
    while true; do
      respaldar || true
      sleep "$((INTERVAL_HOURS * 3600))"
    done ;;
  *) echo "uso: respaldo.sh [ciclo|ahora|restaurar <archivo.dump>]" >&2; exit 2 ;;
esac
