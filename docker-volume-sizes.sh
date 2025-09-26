#!/bin/bash
# Show Docker volumes with sizes and which container(s) use them

echo "Calculating Docker volume sizes..."
echo

# Loop through all volumes
docker volume ls -q | while read v; do
    # Get mountpoint path
    p=$(docker volume inspect "$v" -f '{{ .Mountpoint }}')

    # Get human-readable size
    s=$(sudo du -sh "$p" 2>/dev/null | cut -f1)

    # Find which container(s) use this volume
    containers=$(docker ps -a --filter volume="$v" --format '{{.Names}}')

    # If no container found, mark as "unused"
    if [ -z "$containers" ]; then
        containers="(unused)"
    fi

    # Print nicely
    printf "%-8s %-45s %s\n" "$s" "$v" "$containers"
done | sort -h

