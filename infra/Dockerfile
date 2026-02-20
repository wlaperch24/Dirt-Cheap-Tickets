FROM node:20-bookworm-slim
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-workspace.yaml tsconfig.base.json ./
COPY packages ./packages
COPY services ./services
COPY tests ./tests
RUN pnpm install
RUN pnpm -r build
CMD ["pnpm", "--filter", "@dct/gateway", "dev"]
