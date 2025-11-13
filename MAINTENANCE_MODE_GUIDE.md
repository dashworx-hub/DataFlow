# Maintenance Mode Deployment Guide

This guide explains how to toggle maintenance mode for different deployment platforms.

## How It Works

The app checks maintenance mode in this order:
1. **Environment Variable** `MAINTENANCE_MODE` (takes priority)
2. **Streamlit Secrets** `MAINTENANCE_MODE` (fallback)

**Valid values** (case-insensitive): `"true"`, `"1"`, `"yes"`, `"on"` → Enables maintenance mode
**Any other value or missing** → Disables maintenance mode (normal app)

---

## Deployment Platform Instructions

### 1. Streamlit Cloud (Recommended)

**To Enable Maintenance Mode:**
1. Go to your app on [share.streamlit.io](https://share.streamlit.io)
2. Click **"⋮" (three dots)** → **"Settings"**
3. Go to **"Secrets"** tab
4. Add or edit the secret:
   ```
   MAINTENANCE_MODE = "true"
   ```
5. Click **"Save"** - The app will automatically restart

**To Disable Maintenance Mode:**
1. Go to Settings → Secrets
2. Either:
   - Change the value to `"false"` or `"off"`, OR
   - Delete the `MAINTENANCE_MODE` line entirely
3. Click **"Save"**

**Note:** Changes take effect immediately after saving (app auto-restarts).

---

### 2. Heroku

**To Enable Maintenance Mode:**
```bash
heroku config:set MAINTENANCE_MODE=true --app your-app-name
```

**To Disable Maintenance Mode:**
```bash
heroku config:unset MAINTENANCE_MODE --app your-app-name
```

**Or via Heroku Dashboard:**
1. Go to your app → **Settings** tab
2. Click **"Reveal Config Vars"**
3. Add/Edit: `MAINTENANCE_MODE` = `true` (to enable)
4. Remove the variable or set to `false` (to disable)
5. The app will restart automatically

---

### 3. Docker / Docker Compose

**To Enable Maintenance Mode:**

In `docker-compose.yml`:
```yaml
services:
  streamlit:
    environment:
      - MAINTENANCE_MODE=true
```

Or when running Docker:
```bash
docker run -e MAINTENANCE_MODE=true your-image-name
```

**To Disable:**
- Remove the environment variable or set to `false`
- Restart the container

---

### 4. AWS (Elastic Beanstalk, ECS, EC2)

**Elastic Beanstalk:**
1. Go to your environment → **Configuration** → **Software**
2. Add environment property: `MAINTENANCE_MODE` = `true`
3. Click **"Apply"**

**ECS:**
- Update task definition with environment variable
- Or use AWS Systems Manager Parameter Store

**EC2:**
- Set in `/etc/environment` or in your startup script
- Or use AWS Systems Manager

---

### 5. Google Cloud Platform (Cloud Run, App Engine)

**Cloud Run:**
```bash
gcloud run services update your-service \
  --update-env-vars MAINTENANCE_MODE=true \
  --region your-region
```

**App Engine:**
Add to `app.yaml`:
```yaml
env_variables:
  MAINTENANCE_MODE: "true"
```

---

### 6. Azure (App Service)

**Via Azure Portal:**
1. Go to your App Service → **Configuration** → **Application settings**
2. Add/Edit: `MAINTENANCE_MODE` = `true`
3. Click **"Save"**

**Via Azure CLI:**
```bash
az webapp config appsettings set \
  --name your-app-name \
  --resource-group your-resource-group \
  --settings MAINTENANCE_MODE=true
```

---

### 7. Railway

1. Go to your project → **Variables** tab
2. Add: `MAINTENANCE_MODE` = `true`
3. The app will redeploy automatically

---

### 8. Render

1. Go to your service → **Environment** tab
2. Add environment variable: `MAINTENANCE_MODE` = `true`
3. Click **"Save Changes"** - service will restart

---

### 9. DigitalOcean App Platform

1. Go to your app → **Settings** → **App-Level Environment Variables**
2. Add: `MAINTENANCE_MODE` = `true`
3. Click **"Save"** - app will redeploy

---

### 10. Generic Linux Server / VPS

**Option A: Systemd Service**
Edit your service file (e.g., `/etc/systemd/system/streamlit.service`):
```ini
[Service]
Environment="MAINTENANCE_MODE=true"
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl restart streamlit
```

**Option B: Export in Shell**
Add to your startup script or `.bashrc`:
```bash
export MAINTENANCE_MODE=true
```

---

## Quick Reference

| Platform | Method | Restart Required? |
|----------|--------|-------------------|
| Streamlit Cloud | Secrets UI | Auto (immediate) |
| Heroku | Config Vars | Auto (immediate) |
| Docker | Environment variable | Yes (restart container) |
| AWS | Environment config | Yes |
| GCP | Environment variable | Yes |
| Azure | App Settings | Auto (immediate) |
| Railway | Variables | Auto (redeploy) |
| Render | Environment | Auto (restart) |

---

## Testing After Deployment

After toggling maintenance mode:
1. Wait for the app to restart/redeploy (usually automatic)
2. Refresh your browser (hard refresh: Cmd+Shift+R / Ctrl+Shift+F5)
3. Verify:
   - **Maintenance ON**: Should see maintenance page with message
   - **Maintenance OFF**: Should see normal app with file upload

---

## Troubleshooting

**Maintenance mode not working?**
1. Check the variable name is exactly `MAINTENANCE_MODE` (case-sensitive variable name)
2. Check the value is one of: `"true"`, `"1"`, `"yes"`, `"on"` (case-insensitive value)
3. Ensure the app has restarted after the change
4. Clear browser cache and hard refresh
5. Check platform logs for any errors

**Want to test without deploying?**
- Use the test script: `./test_maintenance.sh on` or `./test_maintenance.sh off`
- Or set locally: `MAINTENANCE_MODE=true streamlit run app.py`

---

## Best Practices

1. **Streamlit Cloud**: Use Secrets UI (easiest, no code changes needed)
2. **Other Platforms**: Use environment variables (standard practice)
3. **Documentation**: Keep this guide updated with your deployment method
4. **Testing**: Always test maintenance mode before going live
5. **Monitoring**: Set up alerts to know when maintenance mode is active

---

## Example: Quick Toggle Script

For platforms with CLI access, you can create a toggle script:

```bash
#!/bin/bash
# toggle_maintenance.sh

CURRENT=$(heroku config:get MAINTENANCE_MODE --app your-app-name 2>/dev/null)

if [ "$CURRENT" = "true" ]; then
    echo "Disabling maintenance mode..."
    heroku config:unset MAINTENANCE_MODE --app your-app-name
else
    echo "Enabling maintenance mode..."
    heroku config:set MAINTENANCE_MODE=true --app your-app-name
fi
```

---

**Need help?** Check your platform's documentation for environment variable management.

