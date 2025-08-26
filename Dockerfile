# Use the official Bun image
FROM oven/bun:1.1.13

# Set the working directory inside the container
WORKDIR /app

# Copy package files and install dependencies
COPY package.json bun.lock ./
RUN bun install

# Copy the rest of your application code
COPY . .

# Start the worker using Bun
CMD ["bun", "index.ts"]