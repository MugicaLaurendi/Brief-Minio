#!/usr/bin/env bash
# Cree 3 policies IAM et 3 comptes de service MinIO (access/secret key dedies,
# sans mot de passe de connexion) :
#   - data-analyst  -> lecture seule sur curated/
#   - data-engineer -> lecture/ecriture sur raw/, staging/ et curated/
#   - admin         -> tous droits (herite du compte root, sans policy restrictive)
#
# Reexecutable sans risque : les policies sont recreees (mc admin policy create
# ecrase la precedente), et la creation d'un service account deja existant
# echoue proprement sans rien casser.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$(cd "$SCRIPT_DIR/.." && pwd)/.env"
CREDS_FILE="$SCRIPT_DIR/credentials.generated.env"
ALIAS=local
ENDPOINT="http://localhost:9000"

# shellcheck disable=SC1090
source "$ENV_FILE"

HAS_LOCAL_MC=0
if [[ -n "$(type -P mc 2>/dev/null)" ]]; then
  HAS_LOCAL_MC=1
fi

MC_CONFIG_DIR="$SCRIPT_DIR/.mc"
mkdir -p "$MC_CONFIG_DIR"

mc() {
  if [[ "$HAS_LOCAL_MC" == "1" ]]; then
    "$(type -P mc)" "$@"
  else
    # -v .mc:/root/.mc : persiste l'alias entre deux invocations docker run
    # distinctes (chaque appel est un conteneur ephemere sans etat partage).
    # Pas de "mc" avant "$@" : l'image minio/mc a deja mc comme ENTRYPOINT,
    # le dupliquer fait echouer la commande silencieusement (exit 0, sans sortie).
    docker run --rm --network host \
      -v "$SCRIPT_DIR/policies:/policies:ro" \
      -v "$MC_CONFIG_DIR:/root/.mc" \
      minio/mc:latest "$@"
  fi
}

# Les chemins de policy doivent pointer vers ce que voit le conteneur mc
# (fallback docker) : reecrit /policies/... -> chemin local si mc est installe.
policy_path() {
  if [[ "$HAS_LOCAL_MC" == "1" ]]; then
    echo "$SCRIPT_DIR/policies/$1"
  else
    echo "/policies/$1"
  fi
}

echo "== Connexion a MinIO ($ENDPOINT) =="
mc alias set "$ALIAS" "$ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null

echo "== Creation des policies =="
mc admin policy create "$ALIAS" data-analyst-readonly "$(policy_path data-analyst-readonly.json)"
mc admin policy create "$ALIAS" data-engineer-readwrite "$(policy_path data-engineer-readwrite.json)"

gen_secret() { openssl rand -hex 20; }

echo "== Creation des comptes de service =="
: > "$CREDS_FILE"
echo "# Genere par setup-service-accounts.sh le $(date -Iseconds) - NE PAS COMMITER" >> "$CREDS_FILE"

create_svcacct() {
  local name="$1" policy_file="$2"
  local secret
  secret="$(gen_secret)"
  local policy_args=()
  if [[ -n "$policy_file" ]]; then
    policy_args=(--policy "$(policy_path "$policy_file")")
  fi
  if mc admin user svcacct add "$ALIAS" "$MINIO_ACCESS_KEY" \
      --access-key "svc-${name}" \
      --secret-key "$secret" \
      --name "$name" \
      --description "Service account ${name} (gere par minio-iam/setup-service-accounts.sh)" \
      "${policy_args[@]}"; then
    {
      echo "MINIO_${name^^}_ACCESS_KEY=svc-${name}"
      echo "MINIO_${name^^}_SECRET_KEY=${secret}"
    } | tr '-' '_' >> "$CREDS_FILE"
  else
    echo "  (svc-${name} existe deja probablement -> non recree, secret non regenere)"
  fi
}

create_svcacct data-analyst data-analyst-readonly.json
create_svcacct data-engineer data-engineer-readwrite.json
create_svcacct admin ""   # pas de --policy => herite des pleins droits du parent (root)

chmod 600 "$CREDS_FILE"
echo
echo "== Termine =="
echo "Identifiants generes dans : $CREDS_FILE (permissions 600, a ne jamais commiter)"
mc admin user svcacct list "$ALIAS" "$MINIO_ACCESS_KEY"
