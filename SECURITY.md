# Security Guidelines

## ⚠️ Before Public Release

This repository contains configuration templates for internal company use. Before making this repository public, ensure:

### 🔐 Sensitive Files to Remove/Update

1. **Configuration Files**

   - Remove all `appsettings.json` and `appsettings.*.json` files
   - Use `appsettings.template.json` instead
   - Remove `db.json` files with actual connection strings

2. **Environment Files**

   - Keep only `.env.example` files with placeholder values
   - Remove all `.env` files with actual credentials

3. **IP Addresses & Hosts**

   - Replace all internal IP addresses (`203.228.x.x`, `10.10.x.x`, `192.168.x.x`)
   - Use `{HOST}`, `{DB_HOST}`, `{K8S_HOST}`, `{REGISTRY_HOST}` placeholders
   - `docker_private_registry/`: Registry/server IPs replaced with `{REGISTRY_HOST}`; `config/openssl-san.cnf` uses `0.0.0.0` placeholder (replace before generating certs)

4. **Credentials**

   - Remove all passwords, API keys, tokens
   - Use `{PASSWORD}`, `{API_KEY}`, `{TOKEN}` placeholders

5. **Email Addresses**

   - Replace company emails with placeholder: `{EMAIL}`
   - Use generic example: `team@example.com`

6. **Target Files (Prometheus/Monitoring)**
   - Remove or sanitize all files in `observability/prometheus/targets/`
   - Use template files with placeholders

### ✅ Required Actions Before Publishing

```bash
# 1. Remove sensitive files from Git history
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch */appsettings.json" \
  --prune-empty -- --all

# 2. Force push (⚠️ WARNING: This rewrites history)
git push origin --force --all

# 3. Clean up local repository
rm -rf .git/refs/original/
git reflog expire --expire=now --all
git gc --prune=now --aggressive
```

### 📋 Checklist

- [x] All passwords removed (or replaced with placeholders)
- [x] All internal IPs replaced with placeholders (incl. `docker_private_registry/`)
- [x] All email addresses sanitized
- [x] Configuration files converted to templates
- [x] `.gitignore` updated
- [ ] Git history cleaned (run `git filter-branch` before public release if sensitive files were ever committed)
- [x] README updated with setup instructions
- [x] Security review completed

### 🔍 Review Status (Last Pass)

- **docker_private_registry**: Docs and config use `{REGISTRY_HOST}`; `openssl-san.cnf` uses `IP.1 = 0.0.0.0` (replace with real IP only when generating certificates on the target server).
- **tr_montrg**: `appsettings.json` / `appsettings.release.json` use `{DB_HOST}`, `{DB_USER}`, `{DB_PASSWORD}`. Ensure `appsettings.json` is in `.gitignore` and not committed.
- **flet_montrg**: All services’ `config.py` and `env.example` use `localhost` for default DB URL (no internal IP). Set `DATABASE_URL` etc. via env in deployment.
- **edge-hmi**: Scripts and README use `{REGISTRY_HOST}` or require registry URL as argument; Gitea URL uses `<GITEA_HOST>` placeholder.
- **unified_montrg_api**: Test `conftest.py` uses placeholder Oracle URL; set `CMMS_DATABASE_URL` in env for real tests.
- **observability/prometheus/targets/**: `.gitignore` has `**/*targets*/*.json`. If these were ever committed, remove from Git history and use template files with placeholders.

### ⚠️ Still to Verify / Optional Cleanup

- **`.env` files**: Ignored by `.gitignore`; if any were committed earlier, remove from history. Do not commit `.env` with real credentials.
- **Default passwords in examples**: `edge-hmi/docker-compose.yml` uses `POSTGRES_PASSWORD:-1q2w3e4r`; `data_pipeline(jj)/dags/dbt/*/profiles.yml` use `DBT_PASSWORD` default `1q2w3e4r!`. Prefer env-only or placeholders in docs.
- **Sample/test IPs**: `data_editor/app/transform_data.py` (172.30.x), `data_pipeline/jj` DAG samples (192.168.8.51, 10.10.100.80), `k8s_guide` (10.10.100.80 as example). Consider placeholders if repo is public.

## 🛡️ Reporting Security Issues

If you discover a security vulnerability, please email: `security@changshininc.com`

**DO NOT** create public GitHub issues for security vulnerabilities.
