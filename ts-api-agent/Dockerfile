FROM node:24-slim

WORKDIR /app

# Copy root workspace manifest
COPY package.json package-lock.json ./
COPY ts-api-agent/package.json ./ts-api-agent/

RUN npm ci --workspace=ts-api-agent

COPY ts-api-agent ./ts-api-agent
COPY tests/fixtures ./tests/fixtures

WORKDIR /app/ts-api-agent

EXPOSE 3000
CMD ["npm", "run", "start"]
