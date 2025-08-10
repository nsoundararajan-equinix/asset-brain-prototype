#!/bin/bash
# Setup script for Spanner emulator in development

set -e

echo "🔧 Setting up Spanner emulator..."

# Wait for emulator to be ready
echo "⏳ Waiting for Spanner emulator to start..."
until curl -f http://spanner-emulator:9010/ > /dev/null 2>&1; do
  sleep 2
done

echo "✅ Spanner emulator is ready"

# Configure gcloud to use emulator
export SPANNER_EMULATOR_HOST=spanner-emulator:9010

# Create instance
echo "📦 Creating Spanner instance..."
gcloud spanner instances create ${SPANNER_INSTANCE_ID} \
    --config=emulator-config \
    --description="Asset Brain Development Instance" \
    --nodes=1

# Create database
echo "🗄️ Creating Spanner database..."
gcloud spanner databases create ${SPANNER_DATABASE_ID} \
    --instance=${SPANNER_INSTANCE_ID}

echo "✅ Spanner emulator setup complete!"
echo "   Instance: ${SPANNER_INSTANCE_ID}"
echo "   Database: ${SPANNER_DATABASE_ID}"
