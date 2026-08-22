# Inventory Service

Manages product inventory and handles stock reservations for the Realtime Order Processing System. This service responds to order creation events by reserving stock, and handles compensation requests when payments fail.

## What It Does

- **Manages products** via REST API (`GET/POST/PUT/PATCH/DELETE /api/products/`)
- **Manages inventory** via REST API (`POST /api/inventory/`, `PATCH /api/inventory/{product_id}/`)
- **Reserves stock** when `orders.created` events arrive
- **Releases stock** when `inventory.release` compensation events arrive (payment failure)
- **Publishes events** to Kafka via the transactional outbox pattern
- **Uses row locking** to prevent overselling in concurrent scenarios

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Inventory Service (:8000)                    │
├─────────────────────────────────────────────────────────────┤
│  Web API (DRF)  │  Kafka Consumer  │  Outbox Publisher      │
│       │         │        │         │        │                │
│       ▼         │        ▼         │        ▼                │
│  InventoryService│  EventHandlers  │  OutboxService         │
│       │         │        │         │        │                │
│       ▼         │        ▼         │        ▼                │
│  OutboxEvent ───────────────────────────────────────────────│
│  EventHistory │  ProcessedEvents (idempotency)              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    Kafka (orders.created, inventory.release, inventory.reserved, inventory.failed)
```

## How to Start

### Prerequisites
- Python 3.12+
- PostgreSQL (via Docker)
- Kafka infrastructure (via Docker)

### 1. Start Dependencies

```bash
# Start Kafka infrastructure (from project root)
cd infra && docker compose up -d

# Start PostgreSQL for this service
cd inventory_service && docker compose up -d
```

### 2. Setup Environment

```bash
cd inventory_service
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run Migrations

```bash
python manage.py migrate
```

### 4. Start the Service

**Option A: All processes via honcho (recommended)**
```bash
honcho start
```
This starts three processes: web (API), consumer (Kafka), publisher (outbox).

**Option B: Individual processes**
```bash
# Terminal 1: Web API
python manage.py runserver 0.0.0.0:8000

# Terminal 2: Kafka Consumer
python manage.py consume_events

# Terminal 3: Outbox Publisher
python manage.py publish_outbox
```

**Option C: VS Code Launch Configs**
Use the provided launch configurations for debugging.

## Dependencies on Other Services

| Dependency | Type | Purpose |
|------------|------|---------|
| **Order Service** | Kafka producer | Sends `orders.created` events to reserve stock |
| **Order Service** | Kafka consumer | Receives `inventory.release` compensation requests |
| **Kafka** | Message broker | Asynchronous communication |
| **PostgreSQL** | Database | Persistent storage for products, inventory, events |
| **Grafana/Loki** | Observability | Log aggregation and monitoring |

### Event Flow

1. **Consumes** `orders.created` → Reserves stock for the order
2. **Publishes** `inventory.reserved` → Order Service (success)
3. **Publishes** `inventory.failed` → Order Service (product not found or out of stock)
4. **Consumes** `inventory.release` → Releases reserved stock (compensation)
5. **Publishes** `inventory.released` → Order Service (compensation complete)

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/products/` | List all products |
| `POST` | `/api/products/` | Create a product |
| `GET` | `/api/products/{id}/` | Get product details |
| `PUT` | `/api/products/{id}/` | Update a product |
| `PATCH` | `/api/products/{id}/` | Partial update a product |
| `DELETE` | `/api/products/{id}/` | Delete a product |
| `POST` | `/api/inventory/` | Create inventory record |
| `PATCH` | `/api/inventory/{product_id}/` | Update inventory quantity |

### Create Product Example

```bash
curl -X POST http://127.0.0.1:8000/api/products/ \
  -H "Content-Type: application/json" \
  -d '{"name": "Laptop", "description": "A laptop", "price": "999.99"}'
```

### Create Inventory Example

```bash
curl -X POST http://127.0.0.1:8000/api/inventory/ \
  -H "Content-Type: application/json" \
  -d '{"product": "<product-uuid>", "available_quantity": 10}'
```

## What Happens When It Reacts

### On `orders.created` (reserve stock)
- **Product not found**: Publishes `inventory.failed` with reason `PRODUCT_NOT_FOUND`
- **Out of stock**: Publishes `inventory.failed` with reason `OUT_OF_STOCK`
- **Success**: Decrements `available_quantity`, publishes `inventory.reserved`
- **Database error**: Raises `RetryableEventException` → routes to retry topic

### On `inventory.release` (compensation)
- Increments `available_quantity` by the reserved quantity
- Publishes `inventory.released` to confirm compensation

### Row Locking (Concurrency Control)
Uses `select_for_update()` with pessimistic row locks to prevent overselling:
```python
inventory = Inventory.objects.select_for_update().get(product_id=product_id)
```
Two concurrent orders for the same product will serialize at the database level, ensuring stock never goes negative.

## Models

| Model | Purpose |
|-------|---------|
| `Product` | Product catalog (name, description, price) |
| `Inventory` | Stock levels per product (available_quantity) |
| `ProcessedEvent` | Idempotency tracking |
| `OutboxEvent` | Transactional outbox for Kafka publishing |
| `EventHistory` | Audit ledger for event lifecycle |

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `postgres://...` | PostgreSQL connection |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SERVICE_NAME` | `inventory-service` | Service identifier in logs |

## Directory Structure

```
inventory_service/
├── config/                     # Django settings, URLs, WSGI
├── core/logging/               # JSON logging infrastructure
│   ├── config.py               # Logging configuration
│   ├── context.py              # contextvars for correlation IDs
│   ├── filters.py              # ContextFilter, HealthCheckFilter
│   ├── formatter.py            # JsonFormatter
│   └── middleware.py           # CorrelationMiddleware
├── inventory/
│   ├── api/                    # DRF views, serializers, URLs
│   ├── events/
│   │   ├── audit/              # EventHistory ledger
│   │   ├── event_envelope.py   # Message contract
│   │   ├── inventory_events.py # Topic constants
│   │   ├── idempotency.py      # ProcessedEvent check/mark
│   │   ├── kafka_consumer.py   # Event handlers
│   │   ├── kafka_publisher.py  # Direct publisher (legacy)
│   │   ├── exceptions.py       # Retryable/NonRetryable
│   │   ├── retry_policy.py     # MAX_RETRIES = 3
│   │   ├── retry_publisher.py  # *.retry producer
│   │   ├── dlq_publisher.py    # *.dlq producer
│   │   ├── failure_handler.py  # Retry/DLQ routing
│   │   └── outbox_service.py   # Outbox CRUD
│   ├── management/commands/
│   │   ├── consume_events.py   # Start Kafka consumer
│   │   └── publish_outbox.py   # Outbox publisher
│   ├── models/                 # Product, Inventory, etc.
│   └── services/
│       └── inventory_service.py # Business logic
├── logs/                       # Structured JSON logs
├── Procfile                    # Process definitions
├── docker-compose.yml          # PostgreSQL container
├── Dockerfile                  # Container image
└── requirements.txt            # Python dependencies
```

## Testing

```bash
python manage.py test
```

Tests cover:
- Product CRUD operations
- Inventory reservation and release
- Row locking behavior
- Event publishing
- Idempotency checks
- Error handling

## Monitoring

Logs are written to `logs/application.log` in JSON format and aggregated by Grafana Alloy → Loki → Grafana.

Filter logs by correlation ID:
```
{service="inventory-service"} |= "<correlation-id>"
```