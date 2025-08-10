#!/bin/bash
# Asset Brain Setup Script
# Sets up the development environment with virtual environment for testing with real Spanner Graph

set -e

echo "🚀 Asset Brain Development Setup"
echo "================================"

# Check Python version
echo "1. Checking Python version..."
python3 --version

# Create virtual environment
echo -e "\n2. Setting up virtual environment..."
if [ ! -d "venv" ]; then
    echo "Creating new virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo -e "\n3. Installing Python dependencies in venv..."
pip install -r requirements.txt

# Install development dependencies
if [ -f "requirements-dev.txt" ]; then
    echo "Installing development dependencies..."
    pip install -r requirements-dev.txt
fi

# Check Google Cloud CLI
echo -e "\n4. Checking Google Cloud CLI..."
if ! command -v gcloud &> /dev/null; then
    echo "❌ Google Cloud CLI not found. Please install it:"
    echo "   https://cloud.google.com/sdk/docs/install"
    exit 1
fi

gcloud --version

# Check authentication
echo -e "\n5. Checking authentication..."
if gcloud auth application-default print-access-token &> /dev/null; then
    echo "✅ Authentication configured"
else
    echo "⚠️ Authentication not configured. Run:"
    echo "   gcloud auth application-default login"
fi

# Environment configuration
echo -e "\n6. Environment configuration..."
if [ -f ".env" ]; then
    echo "✅ .env file exists"
    echo "Current configuration:"
    grep -E "(GOOGLE_CLOUD_PROJECT|SPANNER_INSTANCE_ID|SPANNER_DATABASE_ID)" .env
else
    echo "⚠️ No .env file found. Creating from template..."
    cp .env.example .env
    echo "📝 Please edit .env with your actual values:"
    echo "   GOOGLE_CLOUD_PROJECT=your-project-id"
    echo "   SPANNER_INSTANCE_ID=your-instance-id"
    echo "   SPANNER_DATABASE_ID=asset-brain-dev"
fi

echo -e "\n7. Virtual Environment Created Successfully! ✅"
echo "=========================================="
echo ""
echo "📋 Next Steps:"
echo ""
echo "1. Edit .env with your Spanner configuration"
echo "2. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "3. Test Spanner connection:"
echo "   python test_spanner_connection.py"
echo ""
echo "4. Run ingestion pipeline:"
echo "   cd ingestion && python pipeline.py"
echo ""
echo "5. For Docker deployment (production):"
echo "   docker-compose up --build"
echo ""
echo "💡 Development vs Production:"
echo "   • Local Dev: Use venv (you're in it now!)"
echo "   • Production: Docker containers (no venv needed inside containers)"
echo ""
