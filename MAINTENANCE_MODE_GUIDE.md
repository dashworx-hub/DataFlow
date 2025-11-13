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

---

## Best Practices

1. **Streamlit Cloud**: Use Secrets UI (easiest, no code changes needed)
2. **Other Platforms**: Use environment variables (standard practice)
3. **Documentation**: Keep this guide updated with your deployment method
4. **Testing**: Always test maintenance mode before going live
5. **Monitoring**: Set up alerts to know when maintenance mode is active

---

