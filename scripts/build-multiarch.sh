#!/bin/bash

# Script to configure and build multi-architecture Malachi images
set -e

DOCKER_USERNAME="${DOCKER_USERNAME:-hectorcardoso}"
APP_NAME="malachi"
VERSION=$(grep '@version' mix.exs | head -1 | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/')
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
BUILDER_NAME="${APP_NAME}-builder"

echo "🏗️  Malachi Multi-Architecture Build Script"
echo "=============================================="
echo "Version: $VERSION"
echo "Platforms: $PLATFORMS"
echo "Docker Hub: $DOCKER_USERNAME/$APP_NAME"
echo ""

# Function to check if builder exists
check_builder() {
    if docker buildx inspect "$BUILDER_NAME" &>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to create the builder
create_builder() {
    echo "📦 Creating buildx builder: $BUILDER_NAME"
    docker buildx create --name "$BUILDER_NAME" --use --bootstrap
    docker buildx inspect --bootstrap
    echo "✅ Builder created successfully"
}

# Function to perform build
do_build() {
    echo "🔨 Building multi-architecture image..."
    docker buildx build \
        --builder "$BUILDER_NAME" \
        --platform "$PLATFORMS" \
        -t "$DOCKER_USERNAME/$APP_NAME:$VERSION" \
        -t "$DOCKER_USERNAME/$APP_NAME:latest" \
        --load \
        .
    echo "✅ Build completed"
}

# Function to build and push
do_push() {
    echo "🚀 Building and pushing multi-architecture image..."
    docker buildx build \
        --builder "$BUILDER_NAME" \
        --platform "$PLATFORMS" \
        -t "$DOCKER_USERNAME/$APP_NAME:$VERSION" \
        -t "$DOCKER_USERNAME/$APP_NAME:latest" \
        --push \
        .
    echo "✅ Build and push completed"
    echo ""
    echo "📦 Image pushed to Docker Hub:"
    echo "   docker pull $DOCKER_USERNAME/$APP_NAME:$VERSION"
    echo "   docker pull $DOCKER_USERNAME/$APP_NAME:latest"
}

# Function to inspect image platforms
inspect_image() {
    echo "🔍 Inspecting image on Docker Hub..."
    docker buildx imagetools inspect "$DOCKER_USERNAME/$APP_NAME:latest"
}

# Main menu
case "${1:-}" in
    setup)
        if check_builder; then
            echo "ℹ️  Builder $BUILDER_NAME already exists"
            docker buildx use "$BUILDER_NAME"
        else
            create_builder
        fi
        ;;
    build)
        if ! check_builder; then
            echo "❌ Builder not found. Running setup first..."
            create_builder
        fi
        do_build
        ;;
    push)
        if ! check_builder; then
            echo "❌ Builder not found. Running setup first..."
            create_builder
        fi
        
        # Check if logged in to Docker Hub
        if ! docker info 2>/dev/null | grep -q "Username:"; then
            echo "❌ Not logged in to Docker Hub"
            echo "Please run: docker login"
            exit 1
        fi
        
        do_push
        ;;
    inspect)
        inspect_image
        ;;
    clean)
        if check_builder; then
            echo "🗑️  Removing builder: $BUILDER_NAME"
            docker buildx rm "$BUILDER_NAME"
            echo "✅ Builder removed"
        else
            echo "ℹ️  Builder $BUILDER_NAME does not exist"
        fi
        ;;
    *)
        echo "Usage: $0 {setup|build|push|inspect|clean}"
        echo ""
        echo "Commands:"
        echo "  setup    - Create and configure buildx builder"
        echo "  build    - Build multi-architecture image locally"
        echo "  push     - Build and push to Docker Hub"
        echo "  inspect  - Inspect remote image platforms"
        echo "  clean    - Remove buildx builder"
        echo ""
        echo "Environment variables:"
        echo "  DOCKER_USERNAME - Docker Hub username (default: hectorcardoso)"
        echo "  PLATFORMS       - Target platforms (default: linux/amd64,linux/arm64)"
        echo ""
        echo "Examples:"
        echo "  $0 setup"
        echo "  $0 build"
        echo "  DOCKER_USERNAME=myuser $0 push"
        echo "  $0 inspect"
        exit 1
        ;;
esac
