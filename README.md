# Bitrix Worker

A Bun-based worker for processing deal screenings and handling Pub/Sub messages in Google Cloud Run.

## Features

- Express.js server with proper error handling
- Redis connection management with retry logic
- Graceful shutdown handling for Cloud Run
- Health check endpoints
- Queue processing for deal screenings
- AI-powered deal evaluation

## Deployment to Google Cloud Run

### Prerequisites

1. Google Cloud SDK installed
2. Docker installed
3. Redis instance (Cloud Memorystore or external Redis)

### Environment Variables

Set these environment variables in your Cloud Run service:

- `REDIS_URL`: Redis connection string
- `NODE_ENV`: Set to `production`
- `PORT`: Port number (default: 8080)

### Required GitHub Secrets

Before deploying, make sure these secrets are configured in your GitHub repository:

- `GCP_SA_KEY`: Google Cloud Service Account JSON key
- `REDIS_URL`: Redis connection URL
- `DATABASE_URL`: Database connection URL
- `AI_API_KEY`: AI API key

### Automatic Deployment (Recommended)

The service is automatically deployed using GitHub Actions when you push to the `main` branch.

### Manual Deployment

```bash
# Build the Docker image
docker build -t us-central1-docker.pkg.dev/dark-alpha-deal-sourcing/bitrix24/bitrix-worker:latest .

# Push to Artifact Registry
docker push us-central1-docker.pkg.dev/dark-alpha-deal-sourcing/bitrix24/bitrix-worker:latest

# Deploy to Cloud Run
gcloud run deploy bitrix-worker \
  --image us-central1-docker.pkg.dev/dark-alpha-deal-sourcing/bitrix24/bitrix-worker:latest \
  --region us-central1 \
  --memory 2Gi \
  --allow-unauthenticated
```

## Endpoints

- `GET /` - Root endpoint
- `GET /health` - Health check with Redis status
- `POST /screen-deal` - Process Pub/Sub messages for deal screening
- `POST /process-queue` - Process Redis queue items
- `POST /file-upload` - Handle file uploads

## Troubleshooting

### 503 Connection Termination Errors

The following fixes have been implemented to resolve 503 errors:

1. **Graceful Shutdown**: Proper handling of SIGTERM/SIGINT signals
2. **Redis Connection Management**: Improved connection handling with retry logic
3. **Error Boundaries**: Comprehensive error handling middleware
4. **Health Checks**: Proper health check endpoints for Cloud Run
5. **Resource Management**: Proper cleanup of connections and resources

### Common Issues

1. **Redis Connection Issues**: Ensure `REDIS_URL` is properly set and accessible
2. **Memory Issues**: Increase memory allocation if processing large deals
3. **Timeout Issues**: Increase timeout settings for long-running operations

## Development

```bash
# Install dependencies
bun install

# Start development server
bun run start

# Run Prisma commands
bun run prisma:generate
bun run prisma:migrate
```
