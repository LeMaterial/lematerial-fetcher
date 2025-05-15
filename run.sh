#!/usr/bin/env sh

# FROM: https://github.com/astral-sh/uv-docker-example/blob/main/run.sh

#   --rm                        Remove the container after exiting
#   --volume .:/app             Mount the current directory to `/app` so code changes don't require an image rebuild
#   --volume /app/.venv         Mount the virtual environment separately, so the developer's environment doesn't end up in the container
#   --publish 8000:8000         Expose the web server port 8000 to the host
#   -it $(docker build -q .)    Build the image, then use it as a run target
#   $@                          Pass any arguments to the container

if [ -t 1 ]; then
    INTERACTIVE="-it"
else
    INTERACTIVE=""
fi

docker run \
    --rm \
    -p 5432:5432 \
    -p 3306:3306 \
    --volume .:/app \
    --volume /app/.venv \
    $INTERACTIVE \
    --name lematerial \
    $(docker build -q .) \
    "$@"
