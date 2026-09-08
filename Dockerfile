# Monolithic Dockerfile for EasyProxy
# Optimized EasyProxy runtime
# Compatible with AMD64 and ARM64 (Oracle VPS)

FROM python:3.12-slim-bookworm

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app

# Installa wgcf (generatore config) + wireproxy (tunnel)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && arch=$(dpkg --print-architecture) \
    && case "$arch" in amd64) a="amd64" ;; arm64) a="arm64" ;; *) exit 1 ;; esac \
    && curl -fL "https://github.com/ViRb3/wgcf/releases/download/v2.2.29/wgcf_2.2.29_linux_${a}" -o /usr/local/bin/wgcf \
    && chmod +x /usr/local/bin/wgcf \
    && curl -fL "https://github.com/windtf/wireproxy/releases/download/v1.1.2/wireproxy_linux_${a}.tar.gz" | tar -xz -C /usr/local/bin wireproxy \
    && chmod +x /usr/local/bin/wireproxy

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 7860
CMD ["/bin/bash", "/app/entrypoint.sh"]
