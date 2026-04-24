from functools import lru_cache
from typing import Optional

from baserow.contrib.dashboard.exceptions import DashboardDoesNotExist
from baserow.contrib.dashboard.handler import DashboardHandler
from baserow.contrib.dashboard.widgets.operations import ListWidgetsOperationType
from baserow.core.exceptions import PermissionException
from baserow.core.handler import CoreHandler
from baserow.ws.registries import PageType


@lru_cache(maxsize=1024)
def _workspace_id_for_dashboard(dashboard_id: int) -> Optional[int]:
    """Per-process cache of dashboard_id -> workspace_id."""

    from baserow.contrib.dashboard.models import Dashboard

    try:
        return (
            Dashboard.objects_and_trash.only("id", "workspace_id")
            .get(id=dashboard_id)
            .workspace_id
        )
    except Dashboard.DoesNotExist:
        return None


class DashboardPageType(PageType):
    type = "dashboard"
    parameters = ["dashboard_id"]

    def can_add(self, user, web_socket_id, dashboard_id, **kwargs):
        """
        The user should only have access to this page if the dashboard
        exists and if they have access to it.
        """

        if not dashboard_id:
            return False

        try:
            dashboard = DashboardHandler().get_dashboard(dashboard_id)
            CoreHandler().check_permissions(
                user,
                ListWidgetsOperationType.type,
                workspace=dashboard.workspace,
                context=dashboard,
            )
        except (DashboardDoesNotExist, PermissionException):
            return False

        return True

    def get_group_name(self, dashboard_id, **kwargs):
        return f"dashboard-{dashboard_id}"

    def get_permission_channel_group_name(self, dashboard_id, **kwargs):
        return f"permissions-dashboard-{dashboard_id}"

    def get_workspace_id(self, dashboard_id, **kwargs):
        return _workspace_id_for_dashboard(dashboard_id)
