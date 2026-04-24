from datetime import timedelta
from typing import Optional, Tuple

from django.db.models import Max, Q, Subquery
from django.db.models.expressions import Window
from django.db.models.functions import Coalesce, RowNumber
from django.utils import timezone

from baserow.ws.models import RealtimeUpdate

# Retention bounds for the ws_realtime_updates table. Rows older than the
# retention window or beyond the per-workspace cap (whichever applies first)
# are removed by the periodic celery cleanup task. These live here rather
# than in Django settings because the values are unlikely to need operator
# tuning; promote to env vars only if real-world deployments need it.
REALTIME_UPDATES_RETENTION_HOURS = 24
REALTIME_UPDATES_PER_WORKSPACE_LIMIT = 1000
REALTIME_UPDATES_CLEANUP_INTERVAL_MINUTES = 60


def is_workspace_member(user_id: int, workspace_id: int) -> bool:
    """
    Returns True if the given user is a member of the given workspace. Used to
    authorize ``workspace_realtime_subscribe`` requests without leaking
    workspace existence to non-members.
    """

    from baserow.core.models import WorkspaceUser

    return WorkspaceUser.objects.filter(
        user_id=user_id, workspace_id=workspace_id
    ).exists()


def check_workspace_realtime_updates(
    workspace_id: int,
    last_seen_id: Optional[int],
    previous_web_socket_id: Optional[str],
) -> Tuple[bool, int]:
    """
    Look up whether anything new has been broadcast for ``workspace_id`` since
    ``last_seen_id`` by an originator other than ``previous_web_socket_id``.
    Returns ``(has_updates, current_latest_id)``. When ``last_seen_id`` is
    None, the call is a baseline-only request: ``has_updates`` is always False
    and only ``current_latest_id`` is meaningful.
    """

    current_latest_id = RealtimeUpdate.objects.filter(
        workspace_id=workspace_id
    ).aggregate(latest=Coalesce(Max("id"), 0))["latest"]

    if last_seen_id is None:
        return False, current_latest_id

    # Q(originator_session_id__isnull=True) | ~Q(originator_session_id=...)
    # is equivalent to SQL ``originator_session_id IS DISTINCT FROM %s``:
    # NULL originators (system/Celery broadcasts) count as "someone else".
    other_originator = Q(originator_session_id__isnull=True) | ~Q(
        originator_session_id=previous_web_socket_id
    )
    has_updates = (
        RealtimeUpdate.objects.filter(workspace_id=workspace_id, id__gt=last_seen_id)
        .filter(other_originator)
        .exists()
    )

    return has_updates, current_latest_id


def cleanup_old_realtime_updates(
    retention_hours: int, per_workspace_limit: int
) -> Tuple[int, int]:
    """
    Apply both retention bounds to ``ws_realtime_updates``. Returns a tuple of
    ``(rows_deleted_by_age, rows_deleted_by_per_workspace_cap)``.
    """

    by_age = 0
    by_cap = 0

    if retention_hours and retention_hours > 0:
        cutoff = timezone.now() - timedelta(hours=retention_hours)
        by_age, _ = RealtimeUpdate.objects.filter(created_at__lt=cutoff).delete()

    if per_workspace_limit and per_workspace_limit > 0:
        ids_to_delete = (
            RealtimeUpdate.objects.annotate(
                rn=Window(
                    expression=RowNumber(),
                    partition_by="workspace_id",
                    order_by="-id",
                ),
            )
            .filter(rn__gt=per_workspace_limit)
            .values("id")
        )
        by_cap, _ = RealtimeUpdate.objects.filter(
            id__in=Subquery(ids_to_delete)
        ).delete()

    return by_age, by_cap
