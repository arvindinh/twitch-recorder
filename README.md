# Event-Driven Video Ingestion Pipeline (Twitch Automator)

An enterprise-grade, event-driven Python service that uses Twitch WebSockets to instantly detect live broadcasts, record them in real-time using `streamlink`, and orchestrate nightly uploads to YouTube using Apache Airflow.

## Architecture & Features

- **Real-Time Detection:** Uses Twitch EventSub WebSockets to begin recording the exact second a stream goes live.
- **Automated YouTube Pipeline:** Apache Airflow DAGs automatically split, process, and upload massive VODs to YouTube nightly.
- **Enterprise Observability:** Features JSON structured logging and a live Prometheus metrics server tracking API health and active downloads.
- **Storage Decoupling:** Containerized with Docker and Kubernetes, natively supporting CSI volume mounts to store recordings directly in AWS S3 or other cloud providers without codebase changes.
- **Automated Health Checks:** Weekly Airflow cron jobs actively test API token validity and binary versions to prevent silent failures.

---

## Setup & Configuration

### 1. Prerequisites
- **Docker & Docker Compose** (or a Kubernetes cluster)
- **Python 3.10+** (if running locally without Docker)
- **streamlink** (if running locally)

### 2. Environment Variables
Copy the `.env.example` file to `.env` and configure your settings:
```bash
cp .env.example .env
```
Ensure you provide your `TWITCH_CLIENT_ID` and `TWITCH_CLIENT_SECRET`. The bot will automatically generate and refresh your `TWITCH_USER_TOKEN`.

### 3. YouTube Uploader Configuration (Crucial)
To enable automated YouTube uploads, we use the `youtubeuploader` binary. Talking to the YouTube API requires OAuth2 authentication, which involves generating a `request.token` file. 

**Because this requires a browser popup, you MUST generate the token on a desktop machine first before deploying to a headless server/Kubernetes.**

#### Step A: Configure Google Cloud Console
1. Create a project on the [Google Developers Console](https://console.cloud.google.com/).
2. Navigate to **APIs & Services -> Enable APIs and Services** and enable the **YouTube Data API v3**.
3. Navigate to **APIs & Services -> OAuth Consent Screen** and create a consent screen.
4. Add a test user in **Audience -> Test users** (must be the Google account that owns the YouTube channel).
5. Navigate to **APIs & Services -> Credentials -> Create Credentials -> OAuth client ID** (Select 'Web application').
6. Add an **Authorized redirect URI** of `http://localhost:8080/oauth2callback`.
7. Download the client secrets JSON file and save it exactly as `client_secrets.json` in the root of this project.

#### Step B: Generate the Local Token
1. Download the `youtubeuploader` binary for your OS.
2. Ensure `client_secrets.json` is in the same directory.
3. Run the uploader locally with a dummy file:
   ```bash
   ./youtubeuploader -filename dummy.mp4
   ```
4. A browser window will open asking you to authenticate. Once approved, it will generate a **`request.token`** file in the directory.

### Step C: Deploying to Headless Server
Once `request.token` and `client_secrets.json` are generated locally, copy **both** files to your remote server or mount them as a Kubernetes Secret into the `/app` directory of your Docker container. The automated Airflow DAGs will now have full upload access.

> **Note on Quotas:** Newly created YouTube API projects are subject to strict quotas. By default, you will only be able to upload ~6 videos every 24 hours.

---

## Deployment

### Using Docker Compose (Local/VPS)
1. Ensure your `.env`, `client_secrets.json`, and `request.token` are in the project root.
2. Build and start the background daemon:
```bash
docker-compose up -d --build
```

### Using Kubernetes (Production)
We provide custom Kubernetes manifests in the `/k8s` directory.
1. Edit `k8s/secret.yaml` and populate it with your plaintext credentials (including your Twitch tokens and Airflow username/password). Since the manifest uses `stringData`, Kubernetes handles the base64 encoding for you automatically. Then apply it:
```bash
kubectl apply -f k8s/secret.yaml
```
2. Apply the PersistentVolumeClaim to ensure your recordings persist across container restarts and can be shared with Airflow workers:
```bash
kubectl apply -f k8s/pvc.yaml
```
3. Apply the deployment:
```bash
kubectl apply -f k8s/deployment.yaml
```

---

## Monitoring & Health
- **Prometheus Metrics:** The Python service exposes metrics on port `8000`. Point your Prometheus scraper to `http://<container-ip>:8000/metrics`.
- **Airflow Health Checks:** The Airflow DAG `weekly_system_health_check` runs every Monday at 8:00 AM. It actively validates the Twitch API tokens, checks the `streamlink` and `youtubeuploader` versions, monitors disk space, and executes the `pytest` integration suite to guarantee pipeline health.
