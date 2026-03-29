#!/bin/sh

e5_build_images() {
    local image_name="$1"
    local tag="${2:-local}"  # Default tag to 'local' if not provided

    if [ -z "$image_name" ]; then
        echo "Usage: e5_build_images <image_name> [tag]"
        echo "Example: e5_build_images backend local"
        return 1
    fi

    # Store current directory
    local current_dir="$(pwd)"

    echo "Building image: $image_name:$tag"
    cd backend;
    (
        # Add --progress=plain to see more build output for debugging
        docker build \
            --provenance=false \
            -t "$image_name:$tag" \
            -f "Dockerfile.$image_name" . && \
        docker save "$image_name:$tag" > "$image_name.tar" && \
        microk8s ctr --namespace k8s.io images delete "docker.io/library/$image_name:$tag" 2>/dev/null || true && \
        microk8s ctr --namespace k8s.io image import "$image_name.tar" && \
        microk8s ctr --namespace k8s.io images ls | grep "$image_name" && \
        rm "$image_name.tar"
    )

    # Capture the exit status
    local status=$?

    # Return to original directory
    cd "$current_dir"

    # Check if the build was successful
    if [ $status -eq 0 ]; then
        echo "Successfully built and imported $image_name:$tag"
    else
        echo "Failed to build $image_name:$tag"
        return 1
    fi
}

# Function to build multiple images
e5_build_all_images() {
    local tag="${1:-local}"  # Default tag to 'local' if not provided

    # List of services to build
    local services
    services="products orders"

    # Build each service
    for service in $services; do
        echo "Building $service..."
        if ! e5_build_images "$service" "$tag"; then
            echo "Failed to build $service"
            return 1
        fi
    done

    echo "All images built successfully"
}
