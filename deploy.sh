docker compose up --build
docker compose build taskiq-worker
docker stack deploy uri-discover
docker compose push
