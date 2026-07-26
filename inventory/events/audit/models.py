from django.db import models


class EventHistory(models.Model):
    class Direction(models.TextChoices):
        PUBLISHED = "PUBLISHED", "Published"
        CONSUMED = "CONSUMED", "Consumed"

    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        SUCCESS = "SUCCESS", "Success"
        FAILED = "FAILED", "Failed"

    id = models.BigAutoField(primary_key=True)
    event_id = models.UUIDField(db_index=True)
    event_type = models.CharField(max_length=255)
    service_name = models.CharField(max_length=100)
    correlation_id = models.CharField(max_length=255, db_index=True)
    aggregate_type = models.CharField(max_length=50)
    aggregate_id = models.CharField(max_length=255)
    direction = models.CharField(
        max_length=20,
        choices=Direction.choices,
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
    )
    payload = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "event_history"
        ordering = ["created_at"]
        indexes = [
            models.Index(fields=["correlation_id", "created_at"]),
            models.Index(fields=["aggregate_type", "aggregate_id"]),
        ]

    def __str__(self):
        return f"{self.event_type} [{self.event_id}] — {self.direction} — {self.status}"
