AGGREGATE_INVENTORY = "INVENTORY"

DIRECTION_PUBLISHED = "PUBLISHED"
DIRECTION_CONSUMED = "CONSUMED"

STATUS_PENDING = "PENDING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"

SERVICE_NAME = "inventory-service"


def format_aggregate_id(aggregate_type, entity_id):
    short_id = str(entity_id)[:8]
    return f"{aggregate_type}-{short_id}"
