# Monolithic Dockerfile for EasyProxy
# Optimized EasyProxy runtime
# Compatible with AMD64 and ARM64 (Oracle VPS)

FROM python:3.12-slim-bookworm

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# 1. Solo dipendenze di sistema essenziali + tool per WARP
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    git \
    netcat-openbsd \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 2. WARP userspace (wgcf + wireproxy) — leggero, non richiede privilegi di rete
ARG WGCF_VERSION=2.2.29
ARG WIREPROXY_VERSION=1.1.2
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
        amd64) wgcf_arch="amd64"; wireproxy_arch="amd64" ;; \
        arm64) wgcf_arch="arm64"; wireproxy_arch="arm64" ;; \
        armhf) wgcf_arch="armv7"; wireproxy_arch="arm" ;; \
        *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    curl -fL "https://github.com/ViRb3/wgcf/releases/download/v${WGCF_VERSION}/wgcf_${WGCF_VERSION}_linux_${wgcf_arch}" -o /usr/local/bin/wgcf; \
    chmod +x /usr/local/bin/wgcf; \
    curl -fL "https://github.com/windtf/wireproxy/releases/download/v${WIREPROXY_VERSION}/wireproxy_linux_${wireproxy_arch}.tar.gz" -o /tmp/wireproxy.tar.gz; \
    curl -fL "https://github.com/windtf/wireproxy/releases/download/v${WIREPROXY_VERSION}/checksums.txt" -o /tmp/wireproxy.checksums; \
    checksum="$(awk -v asset="wireproxy_linux_${wireproxy_arch}.tar.gz" '$2 == asset { print $1 }' /tmp/wireproxy.checksums)"; \
    test -n "$checksum"; \
    printf '%s  /tmp/wireproxy.tar.gz\n' "$checksum" | sha256sum -c -; \
    tar -xzf /tmp/wireproxy.tar.gz -C /usr/local/bin wireproxy; \
    chmod +x /usr/local/bin/wireproxy; \
    rm -f /tmp/wireproxy.tar.gz /tmp/wireproxy.checksums; \
    mkdir -p /etc/wireguard

# 3. Dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Codice app
COPY . .

# 5. Permessi
RUN chmod +x entrypoint.sh scripts/warp_userspace_ctl.sh 2>/dev/null || true

EXPOSE 7860
VOLUME ["/data"]

ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
