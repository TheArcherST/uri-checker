```mermaid

sequenceDiagram
        User ->> Web: POST URIs
        Web ->> RabbitMQ: Post tasks (30-100 URIs per task)
        DNS resolver ->> RabbitMQ: Ask tasks
        RabbitMQ ->> DNS resolver: 1-3 tasks (prefetch 1-2)
        
        

```

(not finished)