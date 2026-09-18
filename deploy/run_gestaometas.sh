#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Supervisor simples do Gunicorn (Ubuntu/Linux).
#
#  - Sobe o Gunicorn na raiz do projeto.
#  - Se o Gunicorn cair, espera ESPERA_APOS_QUEDA segundos e sobe de novo.
#  - A cada INTERVALO_REINICIO segundos (padrao 6h) encerra o Gunicorn de
#    forma graciosa (SIGTERM) e sobe de novo, devolvendo toda a memoria ao SO.
#  - Ao receber SIGTERM/SIGINT (kill, Ctrl+C, systemctl stop) encerra o
#    Gunicorn e sai.
#
# Uso:
#   chmod +x deploy/run_gestaometas.sh
#   nohup deploy/run_gestaometas.sh >/dev/null 2>&1 &
#
# Iniciar junto com o servidor (crontab -e):
#   @reboot /home/grupompl/GestaoMetasIndustrial/deploy/run_gestaometas.sh >/dev/null 2>&1
#
# Variaveis opcionais (exportar antes de chamar):
#   APP_DIR              raiz do projeto (padrao: pasta acima de deploy/)
#   VENV                 pasta do virtualenv (padrao: $APP_DIR/venv)
#   INTERVALO_REINICIO   segundos entre reinicios programados (padrao 21600 = 6h)
#   ESPERA_APOS_QUEDA    segundos de espera antes de subir apos uma queda (padrao 5)
#   LOG_DIR              pasta dos logs (padrao: $APP_DIR/logs)
# ---------------------------------------------------------------------------
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="${APP_DIR:-$(dirname "$SCRIPT_DIR")}"
VENV="${VENV:-$APP_DIR/venv}"
INTERVALO_REINICIO="${INTERVALO_REINICIO:-21600}"
ESPERA_APOS_QUEDA="${ESPERA_APOS_QUEDA:-5}"
LOG_DIR="${LOG_DIR:-$APP_DIR/logs}"

mkdir -p "$LOG_DIR"
cd "$APP_DIR" || { echo "Nao foi possivel entrar em $APP_DIR"; exit 1; }

if [ -f "$VENV/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "$VENV/bin/activate"
fi

GUNICORN="$(command -v gunicorn || true)"
if [ -z "$GUNICORN" ]; then
    echo "gunicorn nao encontrado. Rode: pip install -r requirements.txt"
    exit 1
fi

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [supervisor] $*" | tee -a "$LOG_DIR/supervisor.log"
}

PID_GUNICORN=""

encerrar() {
    log "sinal de parada recebido; encerrando gunicorn (pid ${PID_GUNICORN:-?})"
    if [ -n "$PID_GUNICORN" ] && kill -0 "$PID_GUNICORN" 2>/dev/null; then
        kill -TERM "$PID_GUNICORN" 2>/dev/null
        wait "$PID_GUNICORN" 2>/dev/null
    fi
    log "supervisor finalizado"
    exit 0
}
trap encerrar INT TERM

log "supervisor iniciado (APP_DIR=$APP_DIR, reinicio a cada ${INTERVALO_REINICIO}s)"

while true; do
    LOG_APP="$LOG_DIR/gunicorn_$(date '+%Y%m%d_%H%M%S').log"
    log "subindo gunicorn -> $LOG_APP"

    # 'timeout' encerra o gunicorn com SIGTERM apos INTERVALO_REINICIO segundos
    # (reinicio programado) e, se ele nao sair em 90s, manda SIGKILL.
    timeout --signal=TERM --kill-after=90 "$INTERVALO_REINICIO" \
        "$GUNICORN" -c gunicorn.conf.py app_run:app >>"$LOG_APP" 2>&1 &
    PID_GUNICORN=$!

    wait "$PID_GUNICORN"
    CODIGO=$?
    PID_GUNICORN=""

    if [ "$CODIGO" -eq 124 ]; then
        log "reinicio programado apos ${INTERVALO_REINICIO}s"
    else
        log "gunicorn saiu com codigo $CODIGO (queda); novo start em ${ESPERA_APOS_QUEDA}s"
        sleep "$ESPERA_APOS_QUEDA"
    fi
done
