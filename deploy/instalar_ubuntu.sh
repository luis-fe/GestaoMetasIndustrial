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
# Como usar:
#   cd /home/grupompl/GestaoMetasIndustrial
#   chmod +x deploy/*.sh
#   deploy/instalar_ubuntu.sh                      # como o usuario da aplicacao (usa sudo)
#   sudo deploy/instalar_ubuntu.sh --usuario grupompl   # ou como root
#
# Quando rodado como root, o usuario da aplicacao e' o informado em --usuario
# ou, na falta dele, o dono da pasta do projeto. O venv e o pip rodam como
# esse usuario; apt e systemd rodam como root.
#
# Opcoes:
#   --usuario NOME          usuario que roda a aplicacao (padrao: dono da pasta)
#   --python CAMINHO        interpretador Python >= 3.10 a usar no venv
#                           (padrao: o python3.X mais novo encontrado no PATH)
#
# Servidor sem Python >= 3.10 (ex.: Ubuntu 18.04 / Python 3.6): se ja existir
# um venv funcional em $APP_DIR/venv (com Flask e pandas), o instalador o
# REAPROVEITA como esta', sem tocar nos pacotes, e instala apenas o Gunicorn
# na versao compativel. O pip freeze desse venv e' salvo em
# logs/requirements-servidor.txt para referencia.
#   --modo systemd|script   forma de inicializacao automatica (padrao systemd)
#   --sem-apt               nao instalar pacotes do sistema
#   --memoria-max 3G        limite de memoria do servico systemd (padrao 3G)
# ---------------------------------------------------------------------------
set -euo pipefail

MODO="systemd"
USAR_APT=1
APP_USER_OPT=""
PYTHON_OPT=""
PYTHON_MIN="3.10"   # numpy 2.2 / pandas 2.2 / MarkupSafe 3.0 exigem >= 3.10
MEMORIA_MAX="3G"
NOME_SERVICO="gestaometas"

while [ $# -gt 0 ]; do
    case "$1" in
        --usuario) APP_USER_OPT="$2"; shift 2 ;;
        --python) PYTHON_OPT="$2"; shift 2 ;;
        --modo) MODO="$2"; shift 2 ;;
        --sem-apt) USAR_APT=0; shift ;;
        --memoria-max) MEMORIA_MAX="$2"; shift 2 ;;
        -h|--help) sed -n '2,38p' "$0"; exit 0 ;;
        *) echo "Opcao desconhecida: $1"; exit 1 ;;
    esac
done

if [ "$MODO" != "systemd" ] && [ "$MODO" != "script" ]; then
    echo "--modo deve ser 'systemd' ou 'script'"; exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"
VENV="$APP_DIR/venv"

if [ "$(id -u)" -eq 0 ]; then
    SOU_ROOT=1
    SUDO=""
    APP_USER="${APP_USER_OPT:-$(stat -c %U "$APP_DIR")}"
    if ! id "$APP_USER" >/dev/null 2>&1; then
        echo "Usuario '$APP_USER' nao existe. Informe com --usuario NOME."; exit 1
    fi
    if [ "$APP_USER" = "root" ]; then
        echo "AVISO: a aplicacao vai rodar como root. Prefira: --usuario grupompl"
    fi
else
    SOU_ROOT=0
    SUDO="sudo"
    APP_USER="$(id -un)"
    if [ -n "$APP_USER_OPT" ] && [ "$APP_USER_OPT" != "$APP_USER" ]; then
        echo "Para instalar para outro usuario, rode como root: sudo $0 --usuario $APP_USER_OPT"; exit 1
    fi
fi
APP_GROUP="$(id -gn "$APP_USER")"

passo() { echo; echo "==> $*"; }

# Executa um comando como o usuario da aplicacao (ou direto, se ja formos ele).
como_app() {
    if [ "$SOU_ROOT" -eq 1 ] && [ "$APP_USER" != "root" ]; then
        sudo -u "$APP_USER" -H bash -c "$*"
    else
        bash -c "$*"
    fi
}

cd "$APP_DIR"
echo "Projeto : $APP_DIR"
echo "Usuario : $APP_USER"
echo "Modo    : $MODO"

# ---------------------------------------------------------------- 0. python
versao_ok() {  # versao_ok <interpretador> -> 0 se >= PYTHON_MIN
    "$1" -c "import sys; sys.exit(0 if sys.version_info >= tuple(map(int, '$PYTHON_MIN'.split('.'))) else 1)" 2>/dev/null
}

detectar_python() {
    if [ -n "$PYTHON_OPT" ]; then
        command -v "$PYTHON_OPT" >/dev/null 2>&1 || { echo "Python '$PYTHON_OPT' nao encontrado." >&2; exit 1; }
        versao_ok "$PYTHON_OPT" || { echo "Python '$PYTHON_OPT' e' mais antigo que $PYTHON_MIN." >&2; exit 1; }
        echo "$PYTHON_OPT"; return
    fi
    for v in 3.13 3.12 3.11 3.10; do
        if command -v "python$v" >/dev/null 2>&1; then echo "python$v"; return; fi
    done
    if command -v python3 >/dev/null 2>&1 && versao_ok python3; then echo "python3"; return; fi
    echo ""
}

passo "Detectando Python >= $PYTHON_MIN"
REUSAR_VENV=0
PYBIN="$(detectar_python)"
if [ -z "$PYBIN" ] && [ -x "$VENV/bin/python" ] && "$VENV/bin/python" -c "import flask, pandas, sqlalchemy" >/dev/null 2>&1; then
    REUSAR_VENV=1
    PYBIN="$VENV/bin/python"
    echo "Nenhum Python >= $PYTHON_MIN no PATH, mas existe um venv funcional em $VENV"
    echo "($("$PYBIN" --version 2>&1)). Ele sera' REAPROVEITADO sem alterar os pacotes;"
    echo "apenas o Gunicorn sera' instalado."
fi
if [ -z "$PYBIN" ]; then
    echo "Nenhum Python >= $PYTHON_MIN encontrado no PATH (python3 atual: $(python3 --version 2>&1))."
    echo "Instale um e rode de novo, por exemplo:"
    echo "  $SUDO apt-get install -y python3.10 python3.10-venv python3.10-dev"
    echo "Se o apt nao tiver essa versao (Ubuntu 18.04/20.04), adicione o PPA deadsnakes:"
    echo "  $SUDO apt-get install -y software-properties-common"
    echo "  $SUDO add-apt-repository -y ppa:deadsnakes/ppa && $SUDO apt-get update"
    echo "  $SUDO apt-get install -y python3.10 python3.10-venv python3.10-dev"
    echo "Ou aponte para um ja existente: $0 --python /usr/bin/python3.10"
    exit 1
fi
PYBIN="$(command -v "$PYBIN")"
echo "Usando: $PYBIN ($("$PYBIN" --version 2>&1))"
PYNOME="$(basename "$PYBIN")"   # ex.: python3.10

# ---------------------------------------------------------------- 1. apt
if [ "$USAR_APT" -eq 1 ]; then
    passo "Instalando pacotes do sistema"
    $SUDO apt-get update -y
    # libpq-dev/build-essential: compilar psycopg2
    # default-jre-headless: JVM usada pelo JPype/JayDeBeApi (driver Cache JDBC)
    $SUDO apt-get install -y libpq-dev build-essential default-jre-headless
    if [ "$REUSAR_VENV" -eq 0 ]; then
        # venv/dev da versao de Python escolhida (ex.: python3.10-venv). Pode nao
        # existir como pacote se o Python veio de outra fonte; nesse caso so avisa.
        $SUDO apt-get install -y "${PYNOME}-venv" "${PYNOME}-dev" \
            || echo "AVISO: pacotes ${PYNOME}-venv/${PYNOME}-dev nao encontrados no apt; seguindo."
    fi
else
    passo "Pulando instalacao de pacotes do sistema (--sem-apt)"
fi

if ! command -v java >/dev/null 2>&1; then
    echo "AVISO: 'java' nao encontrado. A conexao com o ERP (JDBC) vai falhar."
    echo "       Instale com: sudo apt-get install -y default-jre-headless"
fi

# ---------------------------------------------------------------- 2. venv
if [ "$REUSAR_VENV" -eq 1 ]; then
    passo "Reaproveitando venv existente em $VENV (como $APP_USER)"
    if [ "$SOU_ROOT" -eq 1 ]; then
        chown -R "$APP_USER:$APP_GROUP" "$VENV"
    fi
    # gunicorn 22+ exige Python >= 3.7; para 3.6 a ultima versao e' a 21.2.0
    if "$PYBIN" -c "import sys; sys.exit(0 if sys.version_info >= (3,7) else 1)"; then
        GUNICORN_VER="23.0.0"
    else
        GUNICORN_VER="21.2.0"
    fi
    como_app "'$VENV/bin/pip' install 'gunicorn==$GUNICORN_VER'"
    mkdir -p "$APP_DIR/logs"
    como_app "'$VENV/bin/pip' freeze > '$APP_DIR/logs/requirements-servidor.txt'" || true
    echo "Pacotes do venv salvos em logs/requirements-servidor.txt"
else
    passo "Criando/atualizando virtualenv em $VENV (como $APP_USER)"
    if [ -d "$VENV" ] && [ "$SOU_ROOT" -eq 1 ]; then
        # venv pode ter sido criado por root antes; garante que o usuario da app consiga usar
        chown -R "$APP_USER:$APP_GROUP" "$VENV"
    fi
    # Se ja existe um venv com Python antigo, guarda de lado e recria.
    if [ -f "$VENV/bin/python" ] && ! versao_ok "$VENV/bin/python"; then
        echo "venv existente usa $("$VENV/bin/python" --version 2>&1) (< $PYTHON_MIN); movendo para venv_antigo e recriando."
        rm -rf "$APP_DIR/venv_antigo"
        mv "$VENV" "$APP_DIR/venv_antigo"
    fi
    if [ ! -f "$VENV/bin/activate" ]; then
        como_app "'$PYBIN' -m venv '$VENV'"
    fi
    como_app "'$VENV/bin/pip' install --upgrade pip wheel"
    como_app "'$VENV/bin/pip' install -r '$APP_DIR/requirements.txt'"
fi

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
if [ "$SOU_ROOT" -eq 1 ] && [ "$APP_USER" != "root" ]; then
    chown -R "$APP_USER:$APP_GROUP" "$APP_DIR/logs" "$APP_DIR/dados"
    chown "$APP_USER:$APP_GROUP" "$CONFIG_PY" 2>/dev/null || true
fi

# ---------------------------------------------------------------- 4. boot
if [ "$MODO" = "systemd" ]; then
    passo "Registrando servico systemd '$NOME_SERVICO'"

    # Remove a entrada @reboot do modo script, se existir, para nao subir duas vezes.
    como_app "crontab -l 2>/dev/null | grep -v run_gestaometas.sh | crontab - || true"

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

    $SUDO cp "$UNIT_TMP" "/etc/systemd/system/$NOME_SERVICO.service"
    rm -f "$UNIT_TMP"
    $SUDO systemctl daemon-reload
    $SUDO systemctl enable "$NOME_SERVICO"
    $SUDO systemctl restart "$NOME_SERVICO"

    passo "Status do servico"
    sleep 3
    $SUDO systemctl --no-pager --lines=15 status "$NOME_SERVICO" || true

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
        $SUDO systemctl disable --now "$NOME_SERVICO" || true
    fi

    SUPERVISOR="$SCRIPT_DIR/run_gestaometas.sh"
    LINHA="@reboot $SUPERVISOR >/dev/null 2>&1"
    como_app "(crontab -l 2>/dev/null | grep -v run_gestaometas.sh; echo '$LINHA') | crontab -"

    # Sobe agora, se ainda nao estiver rodando.
    if pgrep -f "run_gestaometas.sh" >/dev/null; then
        echo "Supervisor ja esta em execucao."
    else
        como_app "nohup '$SUPERVISOR' >/dev/null 2>&1 &"
        echo "Supervisor iniciado."
    fi

    echo
    echo "Instalacao concluida (script + crontab)."
    echo "  Logs         : tail -f $APP_DIR/logs/supervisor.log"
    echo "  Parar        : pkill -TERM -f run_gestaometas.sh"
    echo "  Crontab      : crontab -l"
fi
