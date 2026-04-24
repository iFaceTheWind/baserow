import django.db.models
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="RealtimeUpdate",
            fields=[
                (
                    "id",
                    models.BigAutoField(primary_key=True, serialize=False),
                ),
                ("workspace_id", models.IntegerField()),
                ("originator_session_id", models.TextField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "ws_realtime_updates",
                "indexes": [
                    models.Index(
                        fields=["workspace_id", "id"],
                        name="ws_realtime_workspace_id_idx",
                    ),
                    models.Index(
                        fields=["created_at"],
                        name="ws_realtime_created_at_idx",
                    ),
                ],
            },
        ),
        migrations.RunSQL(
            sql="ALTER TABLE ws_realtime_updates SET UNLOGGED;",
            reverse_sql="ALTER TABLE ws_realtime_updates SET LOGGED;",
        ),
    ]
