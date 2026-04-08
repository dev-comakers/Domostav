# Deploy Guide (VPS)

## 1) Server Prerequisites

- Ubuntu 22.04+ (or similar Linux)
- Python 3.9+
- `nginx`
- `systemd`

## 2) Clone Repository

```bash
cd /opt
sudo git clone https://github.com/dev-comakers/Domostav.git domostav-ai
cd /opt/domostav-ai
```

## 3) Setup Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 4) Environment Variables

Create `/opt/domostav-ai/.env`:

```bash
OPENAI_API_KEY=YOUR_KEY
OPENAI_MODEL=gpt-5.4
OPENAI_FALLBACK_MODEL=gpt-4.1-mini
PYTHONIOENCODING=utf-8
PYTHONDONTWRITEBYTECODE=1
```

## 5) Systemd Service

Create `/etc/systemd/system/domostav-ai.service`:

```ini
[Unit]
Description=Domostav AI Flask App
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/domostav-ai
EnvironmentFile=/opt/domostav-ai/.env
ExecStart=/opt/domostav-ai/.venv/bin/python3 /opt/domostav-ai/webapp.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable/start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable domostav-ai
sudo systemctl start domostav-ai
sudo systemctl status domostav-ai
```

## 6) Nginx Reverse Proxy

Create `/etc/nginx/sites-available/domostav-ai`:

```nginx
server {
    listen 80;
    server_name YOUR_DOMAIN_OR_IP;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable:

```bash
sudo ln -s /etc/nginx/sites-available/domostav-ai /etc/nginx/sites-enabled/domostav-ai
sudo nginx -t
sudo systemctl reload nginx
```

## 7) Update Workflow

```bash
cd /opt/domostav-ai
git pull
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart domostav-ai
```

