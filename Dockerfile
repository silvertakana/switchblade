FROM node:24-alpine

WORKDIR /app

ENV NODE_ENV=production
ENV ROUTER_CONFIG=/app/config.server.json

COPY package.json server.mjs index.html config.json config.server.json ./

EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD wget -qO- http://127.0.0.1:8787/health || exit 1

# api.z.ai resolves IPv6-first but the container has no IPv6 route, so outbound
# connections hang and time out; pin it to IPv4 at startup (survives recreates).
CMD ["sh", "-c", "echo '8.217.233.95 api.z.ai' >> /etc/hosts && echo '8.217.100.151 api.z.ai' >> /etc/hosts && exec node server.mjs"]