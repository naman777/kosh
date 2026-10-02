#!/usr/bin/env bash
# Ships the committed code to a VM and (re)starts the stack there.
#   VM=you@1.2.3.4 DOMAIN=lab.example.com deploy/deploy.sh
# DOMAIN is optional; without it the lab is served over plain HTTP on the VM's IP.
set -euo pipefail
: "${VM:?set VM=user@host}"
DOMAIN="${DOMAIN:-:80}"
cd "$(git rev-parse --show-toplevel)"

tarball="$(mktemp -t kosh.XXXXXX).tgz"
git archive --format=tar.gz -o "$tarball" HEAD
scp -q "$tarball" "$VM:kosh.tgz"
rm -f "$tarball"

ssh "$VM" DOMAIN="$DOMAIN" bash -s <<'REMOTE'
set -euo pipefail
mkdir -p ~/kosh
tar xzf ~/kosh.tgz -C ~/kosh
cd ~/kosh
sudo DOMAIN="$DOMAIN" docker compose -f docker-compose.yml -f deploy/compose.prod.yml --profile stack up -d --build --remove-orphans
sudo docker compose -f docker-compose.yml -f deploy/compose.prod.yml --profile stack ps
REMOTE
if [ "$DOMAIN" = ":80" ]; then
  echo "Deployed. The lab is at http://<VM IP>/ and Grafana at http://<VM IP>/grafana/"
else
  echo "Deployed. The lab is at https://$DOMAIN/ and Grafana at https://$DOMAIN/grafana/"
fi
