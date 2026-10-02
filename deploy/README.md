# Deploying the failure lab to a GCP VM

The whole system runs from one compose file on a single VM: Postgres, Redpanda, the lab (which runs every Kosh process and lets visitors kill them), the dashboard, Prometheus, Grafana, and Caddy in front. Only Caddy listens publicly: the dashboard is at `/` and Grafana at `/grafana/`.

## 1. Create the VM

An `e2-standard-2` (2 vCPU, 8 GB) in Mumbai is enough. Run these from a machine with `gcloud` signed in to your project.

```bash
gcloud compute instances create kosh-lab --zone=asia-south1-a --machine-type=e2-standard-2 --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud --boot-disk-size=30GB --tags=kosh-web
```

```bash
gcloud compute firewall-rules create kosh-web --allow=tcp:80,tcp:443 --target-tags=kosh-web
```

Install Docker on it:

```bash
gcloud compute ssh kosh-lab --zone=asia-south1-a --command="curl -fsSL https://get.docker.com | sudo sh"
```

## 2. (Optional) Point a domain at it

Create an `A` record (for example `lab.yourdomain.com`) pointing at the VM's external IP:

```bash
gcloud compute instances describe kosh-lab --zone=asia-south1-a --format="get(networkInterfaces[0].accessConfigs[0].natIP)"
```

With a domain, Caddy gets a Let's Encrypt certificate on first start. Without one, the lab is plain HTTP on the IP.

## 3. Deploy

From the repo, with your SSH user and the VM's IP:

```bash
VM=you@VM_IP DOMAIN=lab.yourdomain.com deploy/deploy.sh
```

The script ships the committed code (`git archive HEAD`), builds the images on the VM and starts the stack. Run it again to deploy a new version; data in Postgres survives.

## Operating it

- Logs: `sudo docker compose -f docker-compose.yml -f deploy/compose.prod.yml --profile stack logs -f lab`
- The lab starts with 2 sales per second of traffic; visitors can raise it to 100.
- Visitors can kill processes. Nothing restarts them automatically except the Start button, by design. If the VM itself restarts, `restart: unless-stopped` brings every container back and the lab restarts all processes.
- Grafana's anonymous access is read-only. Its admin password defaults to `kosh`; set `GRAFANA_ADMIN_PASSWORD` in the environment before deploying to change it.
- Cost: an `e2-standard-2` runs roughly US$50 a month; stop the VM when you're not demoing.
