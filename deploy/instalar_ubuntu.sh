#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Instalador para Ubuntu Server - Gestao de Metas Industriais (Flask/Gunicorn)
#
# O que faz:
#   1. Instala pacotes do sistema (python3-venv, libpq-dev, Java para o JDBC).
#   2. Cria/atualiza o virtualenv em $APP_DIR/venv e instala requirements.txt.
#   3. Confere se _ambiente.env existe e se configApp.localProjeto aponta
#      para a pasta correta (corrige se necessario, com backup).
#   4. Registra a inicializacao automatica no boot:
#        --modo systemd (padrao): servico systemd que sobe o Gunicorn,
#                                 reinicia se cair e a cada 6 horas.
#        --modo script          : entrada @reboot no crontab que chama
#                                 deploy/run_gestaometas.sh (mesmo comportamento,
#                                 para servidores sem systemd).
#   5. Inicia o servico e mostra o status.
#
# Como usar (logado como o usuario que vai rodar a aplicacao, NAO como root):
#   cd /home/grupompl/GestaoMetasIndustrial
#   chmod +x deploy/*.sh
#   deploy/instalar_ubuntu.sh
#
# Opcoes:
#   --modo systemd|script   forma de inicializacao automatica (padrao systemd)
#   --sem-apt               nao instalar pacotes do sistema
#   --memoria-max 3G        limite de memoria do servico systemd (padrao 3G)
#
# O script usa sudo apenas nas etapas que precisam (apt, systemd).
# ---------------------------------------------------------------------------
set -euo pipefail

MODO="systemd"
USAR_APT=1
MEMORIA_MAX="3G"
NOME_SERVICO="gestaometas"

while [ $# -gt 0 ]; do
    case "$1" in
        --modo) MODO="$2"; shift 2 ;;
        --sem-apt) USAR_APT=0; shift ;;
        --memoria-max) MEMORIA_MAX="$2"; shift 2 ;;
        -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
        *) echo "Opcao desconhecida: $1"; exit 1 ;;
    esac
done

if [ "$MODO" != "systemd" ] && [ "$MODO" != "script" ]; then
    echo "--modo deve ser 'systemd' ou 'script'"; exit 1
fi

if [ "$(id -u)" -eq 0 ]; then
    echo "Nao rode este instalador como root. Logue como o usuario da aplicacao;"
    echo "o script pede sudo apenas quando precisar."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"
APP_USER="$(id -un)"
APP_GROUP="$(id -gn)"
VENV="$APP_DIR/venv"

passo() { echo; echo "==> $*"; }

cd "$APP_DIR"
echo "Projeto : $APP_DIR"
echo "Usuario : $APP_USER"
echo "Modo    : $MODO"

# ---------------------------------------------------------------- 1. apt
if [ "$USAR_APT" -eq 1 ]; then
    passo "Instalando pacotes do sistema (sudo)"
    sudo apt-get update -y
    # python3-dev/libpq-dev/build-essential: compilar psycopg2
    # default-jre-headless: JVM usada pelo JPype/JayDeBeApi (driver Cache JDBC)
    sudo apt-get install -y python3 python3-venv python3-dev python3-pip \
        libpq-dev build-essential default-jre-headless
else
    passo "Pulando instalacao de pacotes do sistema (--sem-apt)"
fi

if ! command -v java >/dev/null 2>&1; then
    echo "AVISO: 'java' nao encontrado. A conexao com o ERP (JDBC) vai falhar."
    echo "       Instale com: sudo apt-get install -y default-jre-headless"
fi

# ---------------------------------------------------------------- 2. venv
passo "Criando/atualizando virtualenv em $VENV"
if [ ! -f "$VENV/bin/activate" ]; then
    python3 -m venv "$VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install --upgrade pip wheel
pip install -r "$APP_DIR/requirements.txt"

# ---------------------------------------------------------------- 3. config
passo "Conferindo configuracao"
if [ ! -f "$APP_DIR/_ambiente.env" ]; then
    echo "AVISO: $APP_DIR/_ambiente.env nao existe."
    echo "       Crie-o com as variaveis descritas no README antes de iniciar o servico."
fi

CONFIG_PY="$APP_DIR/src/configApp/configApp.py"
ATUAL="$(grep -oP 'localProjeto\s*=\s*"\K[^"]+' "$CONFIG_PY" || true)"
if [ "$ATUAL" != "$APP_DIR" ]; then
    echo "configApp.localProjeto = \"$ATUAL\" -> ajustando para \"$APP_DIR\" (backup em configApp.py.bak)"
    cp "$CONFIG_PY" "$CONFIG_PY.bak"
    sed -i "s|localProjeto\s*=\s*\".*\"|localProjeto = \"$APP_DIR\"|" "$CONFIG_PY"
else
    echo "configApp.localProjeto ok ($APP_DIR)"
fi

chmod +x "$SCRIPT_DIR"/*.sh
mkdir -p "$APP_DIR/logs" "$APP_DIR/dados/backup"

# ---------------------------------------------------------------- 4. boot
if [ "$MODO" = "systemd" ]; then
    passo "Registrando servico systemd '$NOME_SERVICO' (sudo)"

    # Remove a entrada @reboot do modo script, se existir, para nao subir duas vezes.
    (crontab -l 2>/dev/null | grep -v "run_gestaometas.sh" | crontab -) || true

    UNIT_TMP="$(mktemp)"
    cat > "$UNIT_TMP" <<UNIT
[Unit]
Description=Gestao de Metas Industriais - Flask/Gunicorn
After=network-online.target
Wants=network-online.target

[Service]
User=$APP_USER
Group=$APP_GROUP
WorkingDirectory=$APP_DIR
Environment="PATH=$VENV/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=$VENV/bin/gunicorn -c gunicorn.conf.py app_run:app
ExecReload=/bin/kill -s HUP \$MAINPID
Restart=always
RestartSec=5
# Reinicio programado a cada 6h (libera memoria retida pelo pandas/JVM).
RuntimeMaxSec=6h
KillMode=mixed
TimeoutStopSec=90
MemoryMax=$MEMORIA_MAX

[Install]
WantedBy=multi-user.target
UNIT

    sudo cp "$UNIT_TMP" "/etc/systemd/system/$NOME_SERVICO.service"
    rm -f "$UNIT_TMP"
    sudo systemctl daemon-reload
    sudo systemctl enable "$NOME_SERVICO"
    sudo systemctl restart "$NOME_SERVICO"

    passo "Status do servico"
    sleep 3
    sudo systemctl --no-pager --lines=15 status "$NOME_SERVICO" || true

    echo
    echo "Instalacao concluida (systemd)."
    echo "  Logs ao vivo : journalctl -u $NOME_SERVICO -f"
    echo "  Parar        : sudo systemctl stop $NOME_SERVICO"
    echo "  Reiniciar    : sudo systemctl restart $NOME_SERVICO"
    echo "  Apos deploy  : pip install -r requirements.txt && sudo systemctl restart $NOME_SERVICO"

else
    passo "Registrando inicializacao via crontab @reboot (script supervisor)"

    # Se o servico systemd existir de uma instalacao anterior, desativa.
    if systemctl list-unit-files 2>/dev/null | grep -q "^$NOME_SERVICO.service"; then
        sudo systemctl disable --now "$NOME_SERVICO" || true
    fi

    SUPERVISOR="$SCRIPT_DIR/run_gestaometas.sh"
    LINHA="@reboot $SUPERVISOR >/dev/null 2>&1"
    (crontab -l 2>/dev/null | grep -v "run_gestaometas.sh"; echo "$LINHA") | crontab -

    # Sobe agora, se ainda nao estiver rodando.
    if pgrep -f "run_gestaometas.sh" >/dev/null; then
        echo "Supervisor ja esta em execucao."
    else
        nohup "$SUPERVISOR" >/dev/null 2>&1 &
        echo "Supervisor iniciado (pid $!)."
    fi

    echo
    echo "Instalacao concluida (script + crontab)."
    echo "  Logs         : tail -f $APP_DIR/logs/supervisor.log"
    echo "  Parar        : pkill -TERM -f run_gestaometas.sh"
    echo "  Crontab      : crontab -l"
fi
