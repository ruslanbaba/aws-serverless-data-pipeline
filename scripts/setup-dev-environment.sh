#!/bin/bash

# Setup Development Environment for AWS Serverless Data Pipeline
# This script sets up the complete development environment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check if running on macOS or Linux
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        OS="linux"
    else
        log_error "Unsupported operating system: $OSTYPE"
        exit 1
    fi
    log_info "Detected OS: $OS"
}

# Install system dependencies
install_system_dependencies() {
    log_info "Installing system dependencies..."
    
    if [[ "$OS" == "macos" ]]; then
        # Check if Homebrew is installed
        if ! command -v brew &> /dev/null; then
            log_info "Installing Homebrew..."
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        fi
        
        # Install dependencies
        brew update
        brew install python@3.11 node jq aws-cli docker
        
    elif [[ "$OS" == "linux" ]]; then
        # Ubuntu/Debian
        if command -v apt-get &> /dev/null; then
            sudo apt-get update
            sudo apt-get install -y python3.11 python3-pip nodejs npm jq awscli docker.io curl wget
        # CentOS/RHEL/Amazon Linux
        elif command -v yum &> /dev/null; then
            sudo yum update -y
            sudo yum install -y python3 python3-pip nodejs npm jq awscli docker curl wget
        else
            log_warning "Package manager not recognized. Please install dependencies manually."
        fi
    fi
    
    log_success "System dependencies installed"
}

# Setup Python environment
setup_python_environment() {
    log_info "Setting up Python virtual environment..."
    
    cd "$PROJECT_ROOT"
    
    # Create virtual environment
    python3 -m venv venv
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip setuptools wheel
    
    # Install development dependencies
    cat > requirements-dev.txt << 'EOF'
# Development dependencies
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-mock>=3.11.0
black>=23.7.0
flake8>=6.0.0
mypy>=1.5.0
bandit>=1.7.5
safety>=2.3.0
pre-commit>=3.3.0
sphinx>=7.1.0
boto3-stubs[essential]>=1.28.0
moto>=4.2.0
localstack-client>=1.40.0
EOF
    
    pip install -r requirements-dev.txt
    
    # Install Lambda function dependencies
    for lambda_dir in lambda/*/; do
        if [[ -f "$lambda_dir/requirements.txt" ]]; then
            log_info "Installing dependencies for $(basename "$lambda_dir")"
            pip install -r "$lambda_dir/requirements.txt"
        fi
    done
    
    log_success "Python environment setup complete"
}

# Setup Node.js environment
setup_node_environment() {
    log_info "Setting up Node.js environment..."
    
    cd "$PROJECT_ROOT"
    
    # Install Node.js dependencies
    npm install
    
    # Setup Git hooks
    npm run prepare
    
    log_success "Node.js environment setup complete"
}

# Configure AWS CLI
configure_aws_cli() {
    log_info "Configuring AWS CLI..."
    
    if ! aws sts get-caller-identity &> /dev/null; then
        log_warning "AWS CLI not configured. Please run 'aws configure' manually."
        log_info "You'll need:"
        log_info "  - AWS Access Key ID"
        log_info "  - AWS Secret Access Key"
        log_info "  - Default region (e.g., us-east-1)"
        log_info "  - Default output format (json recommended)"
    else
        log_success "AWS CLI already configured"
        aws sts get-caller-identity --output table
    fi
}

# Setup Git hooks
setup_git_hooks() {
    log_info "Setting up Git hooks..."
    
    cd "$PROJECT_ROOT"
    
    # Pre-commit hook
    cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
# Pre-commit hook for code quality checks

set -e

echo "Running pre-commit checks..."

# Activate virtual environment if it exists
if [[ -f "venv/bin/activate" ]]; then
    source venv/bin/activate
fi

# Python code formatting
if command -v black &> /dev/null; then
    echo "Running Black formatter..."
    black --check --diff lambda/ glue/ tests/ || {
        echo "Code formatting issues found. Run 'black lambda/ glue/ tests/' to fix."
        exit 1
    }
fi

# Python linting
if command -v flake8 &> /dev/null; then
    echo "Running flake8 linter..."
    flake8 lambda/ glue/ tests/
fi

# Security scan
if command -v bandit &> /dev/null; then
    echo "Running security scan..."
    bandit -r lambda/ glue/ -f json -o /tmp/bandit-report.json || {
        echo "Security issues found. Check the report."
        exit 1
    }
fi

# Type checking
if command -v mypy &> /dev/null; then
    echo "Running type checking..."
    mypy lambda/ glue/ || {
        echo "Type checking failed."
        exit 1
    }
fi

echo "Pre-commit checks passed!"
EOF

    chmod +x .git/hooks/pre-commit
    
    # Pre-push hook
    cat > .git/hooks/pre-push << 'EOF'
#!/bin/bash
# Pre-push hook for comprehensive testing

set -e

echo "Running pre-push checks..."

# Run unit tests
if [[ -d "tests/" ]]; then
    echo "Running unit tests..."
    pytest tests/unit/ -v --cov=lambda --cov=glue --cov-report=term-missing
fi

# Validate CloudFormation templates
echo "Validating CloudFormation templates..."
for template in infrastructure/*.yaml; do
    if [[ -f "$template" ]]; then
        echo "Validating $(basename "$template")..."
        aws cloudformation validate-template --template-body "file://$template" > /dev/null
    fi
done

echo "Pre-push checks passed!"
EOF

    chmod +x .git/hooks/pre-push
    
    log_success "Git hooks setup complete"
}

# Create development configuration
create_dev_config() {
    log_info "Creating development configuration..."
    
    cd "$PROJECT_ROOT"
    
    # Create .env file for local development
    cat > .env.example << 'EOF'
# Development Environment Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default
ENVIRONMENT=dev
PROJECT_NAME=healthcare-claims-pipeline

# Local development settings
LOCAL_STACK_ENDPOINT=http://localhost:4566
DEBUG_MODE=true
LOG_LEVEL=DEBUG

# S3 Configuration
DATA_BUCKET_PREFIX=healthcare-claims-dev
DEPLOYMENT_BUCKET=deployment-artifacts-dev

# Monitoring
MONITORING_EMAIL=dev-team@company.com
ALERT_EMAIL=alerts-dev@company.com

# Performance settings
LAMBDA_MEMORY_SIZE=512
LAMBDA_TIMEOUT=300
GLUE_MAX_CAPACITY=10
EOF

    # Create local config directory
    mkdir -p config/local
    
    # Create local override config
    cat > config/local/dev.yaml << 'EOF'
# Local development overrides
environment: dev
region: us-east-1

# Local services
services:
  localstack:
    enabled: false
    endpoint: http://localhost:4566
  
# Development settings
development:
  hot_reload: true
  debug_mode: true
  mock_external_services: true
  
# Testing
testing:
  use_moto: true
  create_test_data: true
  cleanup_after_tests: true
EOF

    log_success "Development configuration created"
}

# Setup documentation environment
setup_documentation() {
    log_info "Setting up documentation environment..."
    
    cd "$PROJECT_ROOT"
    
    # Create docs directory structure
    mkdir -p docs/{api,guides,diagrams,examples}
    
    # Setup Sphinx for Python documentation
    if command -v sphinx-quickstart &> /dev/null; then
        cd docs/
        echo -e "n\n.\n.\nHealthcare Claims Pipeline\nDev Team\n1.0\nen\n.\n.\ny\ny\ny\ny\nn\ny\ny\ny\ny\nn\ny\n" | sphinx-quickstart
        cd ..
    fi
    
    log_success "Documentation environment setup complete"
}

# Validate installation
validate_installation() {
    log_info "Validating installation..."
    
    local errors=0
    
    # Check Python
    if ! python3 --version &> /dev/null; then
        log_error "Python 3 not found"
        ((errors++))
    else
        log_success "Python 3: $(python3 --version)"
    fi
    
    # Check Node.js
    if ! node --version &> /dev/null; then
        log_error "Node.js not found"
        ((errors++))
    else
        log_success "Node.js: $(node --version)"
    fi
    
    # Check AWS CLI
    if ! aws --version &> /dev/null; then
        log_error "AWS CLI not found"
        ((errors++))
    else
        log_success "AWS CLI: $(aws --version)"
    fi
    
    # Check jq
    if ! jq --version &> /dev/null; then
        log_error "jq not found"
        ((errors++))
    else
        log_success "jq: $(jq --version)"
    fi
    
    # Check Docker
    if ! docker --version &> /dev/null; then
        log_warning "Docker not found (optional for local testing)"
    else
        log_success "Docker: $(docker --version)"
    fi
    
    # Check virtual environment
    if [[ -d "venv" ]]; then
        log_success "Python virtual environment created"
    else
        log_error "Python virtual environment not found"
        ((errors++))
    fi
    
    if [[ $errors -gt 0 ]]; then
        log_error "Installation validation failed with $errors errors"
        return 1
    else
        log_success "Installation validation passed"
        return 0
    fi
}

# Main setup function
main() {
    log_info "Starting development environment setup..."
    log_info "Project root: $PROJECT_ROOT"
    
    detect_os
    install_system_dependencies
    setup_python_environment
    setup_node_environment
    configure_aws_cli
    setup_git_hooks
    create_dev_config
    setup_documentation
    
    if validate_installation; then
        log_success "Development environment setup complete!"
        echo
        log_info "Next steps:"
        log_info "1. Copy .env.example to .env and configure your settings"
        log_info "2. Configure AWS CLI: aws configure"
        log_info "3. Test the setup: ./scripts/test-pipeline.sh --test-type unit"
        log_info "4. Deploy to dev: ./scripts/deploy.sh --environment dev --bucket your-bucket"
        echo
        log_info "Documentation:"
        log_info "- Technical docs: docs/technical-documentation.md"
        log_info "- Contributing: docs/CONTRIBUTING.md"
        log_info "- Security: security/security-policies.md"
    else
        log_error "Setup completed with errors. Please review and fix the issues above."
        exit 1
    fi
}

# Run main function
main "$@"
