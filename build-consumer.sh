#!/bin/bash

# Build and deployment script for Advanced Kafka Consumer with Dynatrace
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="advanced-kafka-consumer"
IMAGE_TAG="latest"
CONTAINER_NAME="advanced-kafka-consumer"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Function to load environment variables
load_env() {
    if [ -f "$SCRIPT_DIR/.env.docker" ]; then
        print_status "Loading environment variables from .env.docker"
        set -a
        source "$SCRIPT_DIR/.env.docker"
        set +a
    else
        print_warning ".env.docker file not found. Using default values."
    fi
}

# Function to validate Dynatrace configuration
validate_dynatrace() {
    if [ -n "$DT_TENANT" ] && [ -n "$DT_PAAS_TOKEN" ]; then
        print_success "Dynatrace configuration found"
        return 0
    else
        print_warning "Dynatrace configuration not complete. OneAgent will not be installed."
        return 1
    fi
}

# Function to build the Docker image
build_image() {
    print_status "Building Docker image: $IMAGE_NAME:$IMAGE_TAG"
    
    # Build arguments for Dynatrace
    BUILD_ARGS=""
    if [ -n "$DT_TENANT" ]; then
        BUILD_ARGS="$BUILD_ARGS --build-arg DT_TENANT=$DT_TENANT"
    fi
    if [ -n "$DT_API_TOKEN" ]; then
        BUILD_ARGS="$BUILD_ARGS --build-arg DT_API_TOKEN=$DT_API_TOKEN"
    fi
    if [ -n "$DT_PAAS_TOKEN" ]; then
        BUILD_ARGS="$BUILD_ARGS --build-arg DT_PAAS_TOKEN=$DT_PAAS_TOKEN"
    fi
    if [ -n "$DT_CONNECTION_POINT" ]; then
        BUILD_ARGS="$BUILD_ARGS --build-arg DT_CONNECTION_POINT=$DT_CONNECTION_POINT"
    fi
    
    # Build the image
    docker build $BUILD_ARGS -t "$IMAGE_NAME:$IMAGE_TAG" "$SCRIPT_DIR"
    
    if [ $? -eq 0 ]; then
        print_success "Docker image built successfully"
    else
        print_error "Failed to build Docker image"
        exit 1
    fi
}

# Function to run the container
run_container() {
    print_status "Starting container: $CONTAINER_NAME"
    
    # Stop existing container if running
    if docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
        print_status "Stopping existing container"
        docker stop "$CONTAINER_NAME"
        docker rm "$CONTAINER_NAME"
    fi
    
    # Run the container
    docker run -d \
        --name "$CONTAINER_NAME" \
        --restart unless-stopped \
        --env-file "$SCRIPT_DIR/.env.docker" \
        -p 8080:8080 \
        -v consumer-logs:/app/logs \
        --network kafka-network \
        "$IMAGE_NAME:$IMAGE_TAG"
    
    if [ $? -eq 0 ]; then
        print_success "Container started successfully"
        print_status "Container ID: $(docker ps -q -f name=$CONTAINER_NAME)"
    else
        print_error "Failed to start container"
        exit 1
    fi
}

# Function to show container logs
show_logs() {
    print_status "Showing container logs (press Ctrl+C to exit)"
    docker logs -f "$CONTAINER_NAME"
}

# Function to show container status
show_status() {
    print_status "Container Status:"
    docker ps -f name="$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    
    print_status "Container Health:"
    docker inspect "$CONTAINER_NAME" --format='{{.State.Health.Status}}' 2>/dev/null || echo "No health check configured"
    
    print_status "Resource Usage:"
    docker stats "$CONTAINER_NAME" --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"
}

# Function to stop the container
stop_container() {
    print_status "Stopping container: $CONTAINER_NAME"
    docker stop "$CONTAINER_NAME"
    docker rm "$CONTAINER_NAME"
    print_success "Container stopped and removed"
}

# Function to clean up Docker resources
cleanup() {
    print_status "Cleaning up Docker resources"
    
    # Remove stopped containers
    docker container prune -f
    
    # Remove unused images
    docker image prune -f
    
    # Remove unused volumes
    docker volume prune -f
    
    print_success "Cleanup completed"
}

# Function to run with Docker Compose
compose_up() {
    print_status "Starting services with Docker Compose"
    
    # Ensure the main Kafka stack is running
    if ! docker network ls | grep -q kafka-network; then
        print_status "Creating Kafka network"
        docker network create kafka-network
    fi
    
    # Start the main Kafka stack first
    docker-compose up -d
    
    # Wait for Kafka to be ready
    print_status "Waiting for Kafka to be ready..."
    sleep 30
    
    # Start the consumer
    docker-compose -f docker-compose.consumer.yml up -d
    
    print_success "All services started"
}

# Function to stop Docker Compose services
compose_down() {
    print_status "Stopping Docker Compose services"
    docker-compose -f docker-compose.consumer.yml down
    docker-compose down
    print_success "Services stopped"
}

# Function to show help
show_help() {
    echo "Advanced Kafka Consumer Build and Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  build       Build the Docker image"
    echo "  run         Run the container"
    echo "  logs        Show container logs"
    echo "  status      Show container status"
    echo "  stop        Stop and remove the container"
    echo "  restart     Restart the container (stop + run)"
    echo "  cleanup     Clean up Docker resources"
    echo "  compose-up  Start all services with Docker Compose"
    echo "  compose-down Stop all Docker Compose services"
    echo "  full        Full deployment (build + compose-up)"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build                 # Build the Docker image"
    echo "  $0 full                  # Complete deployment"
    echo "  $0 logs                  # View container logs"
    echo "  $0 status                # Check container status"
}

# Main script logic
main() {
    case "${1:-help}" in
        build)
            check_docker
            load_env
            validate_dynatrace
            build_image
            ;;
        run)
            check_docker
            load_env
            run_container
            ;;
        logs)
            check_docker
            show_logs
            ;;
        status)
            check_docker
            show_status
            ;;
        stop)
            check_docker
            stop_container
            ;;
        restart)
            check_docker
            load_env
            stop_container
            run_container
            ;;
        cleanup)
            check_docker
            cleanup
            ;;
        compose-up)
            check_docker
            load_env
            compose_up
            ;;
        compose-down)
            check_docker
            compose_down
            ;;
        full)
            check_docker
            load_env
            validate_dynatrace
            build_image
            compose_up
            print_success "Full deployment completed!"
            print_status "Use '$0 logs' to view container logs"
            print_status "Use '$0 status' to check container status"
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            print_error "Unknown command: $1"
            show_help
            exit 1
            ;;
    esac
}

# Run the main function
main "$@"
