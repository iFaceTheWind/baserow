from django.db import models


class RealtimeUpdate(models.Model):
    id = models.BigAutoField(primary_key=True)
    workspace_id = models.IntegerField()
    originator_session_id = models.TextField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "ws_realtime_updates"
        indexes = [
            models.Index(
                fields=["workspace_id", "id"], name="ws_realtime_workspace_id_idx"
            ),
            models.Index(fields=["created_at"], name="ws_realtime_created_at_idx"),
        ]
