FROM node:24-alpine

WORKDIR /app

ENV NODE_ENV=production
ENV ROUTER_CONFIG=/app/config.server.json

COPY package.json server.mjs index.html config.json config.server.json ./

EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD wget -qO- http://127.0.0.1:8787/health || exit 1

CMD ["node", "server.mjs"]