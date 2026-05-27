# Notification Service

Orchestration service that consumes Kafka events and delivers notifications through multiple channels.

## Channels
- **WebSocket** — real-time push to connected clients
- **Email** — publishes commands to `notification.email` Kafka topic (consumed by mail-service)
- **Push** — publishes commands to `notification.push` Kafka topic

## Architecture

```
Kafka Events -> ProcessEvent
                 |
                 v
         NotificationRepository (PostgreSQL)
                 |
                 v
         [WebSocket Hub] -> connected clients
         [Kafka Producer] -> notification.email / notification.push
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `HTTP_ADDRESS` | `:8087` | HTTP listen address |
| `DB_DSN` | (required) | PostgreSQL DSN |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka brokers |
| `KAFKA_CONSUMER_GROUP` | `notification-service` | Kafka consumer group ID |
| `LOG_LEVEL` | `info` | Logging level (debug, info, warn, error) |

## HTTP API

All endpoints use `X-User-ID` header for user identification.

```
GET    /internal/health                    # Health check
GET    /internal/notifications             # List notifications (limit, offset)
GET    /internal/notifications/unread-count # Unread count
POST   /internal/notifications/{id}/read   # Mark as read
POST   /internal/notifications/read-all    # Mark all as read
DELETE /internal/notifications/{id}        # Delete notification
GET    /ws                                  # WebSocket (query param: ?user_id=UUID)
```

## WebSocket Messages

Server -> Client:
```json
{"type": "notification.new", "notification": {...}}
{"type": "notification.unread_count", "count": 5}
```

## Kafka Topics

### Consumed
- `offer.created`, `offer.accepted`, `offer.rejected`, `offer.withdrawn`, `offer.countered`
- `chat.message.sent`
- `order.assigned`, `order.completed`, `order.cancelled`
- `review.created`
- `complaint.created`, `complaint.updated`
- `user.verification.created`, `user.created`

### Produced
- `notification.email` — email delivery commands
- `notification.push` — push notification commands
- `notification.created` — notification lifecycle events

## Running

```bash
# Local
go run ./cmd/api/

# Docker
docker build -t notification-service .
docker run -e DB_DSN=postgres://... -e KAFKA_BROKERS=kafka:9092 notification-service
```
