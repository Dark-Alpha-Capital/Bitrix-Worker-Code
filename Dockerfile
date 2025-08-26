# Use the official Bun image
FROM oven/bun:1.1.13

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy package files and install dependencies
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# Copy Prisma schema and generate client
COPY prisma ./prisma/
RUN bun run prisma:generate

# Copy the rest of your application code
COPY . .

# Create a non-root user for security
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 bun
USER bun

# Expose the port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Start the worker using Bun with production script
CMD ["bun", "run", "start:prod"]