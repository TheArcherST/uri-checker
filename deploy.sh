docker compose up --build -d
docker compose build taskiq-worker
docker compose push taskiq-worker &
docker stack deploy -c swarm-stack.yml uri-discover
docker compose push taskiq-worker
