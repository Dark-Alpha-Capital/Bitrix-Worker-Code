# Use the official Bun image
FROM oven/bun:1.1.13-alpine

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies
RUN apk add --no-cache \
    curl \
    ca-certificates

# Copy package files and install dependencies
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

# Copy Prisma schema and generate client
COPY prisma ./prisma/
RUN bun run prisma:generate

# Copy the rest of your application code
COPY . .



EXPOSE 8080



# Start the worker using Bun with production script
CMD ["bun", "run", "start:prod"]