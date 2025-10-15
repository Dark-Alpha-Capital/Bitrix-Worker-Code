# Use the official Bun image
FROM oven/bun:1.1.13

# Set the working directory inside the container
WORKDIR /app

# Copy package files and install dependencies
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# Copy Prisma schema and generate client
COPY prisma ./prisma/
RUN bun run prisma:generate

# Copy the rest of your application code
COPY . .

# Expose the port
EXPOSE 8080

# Start the worker using Bun with production script
CMD ["bun", "run", "start:prod"]