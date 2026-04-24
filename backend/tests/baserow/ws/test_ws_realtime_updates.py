import pytest
from asgiref.sync import sync_to_async
from channels.testing import WebsocketCommunicator

from baserow.config.asgi import application
from baserow.ws.realtime_updates import (
    check_workspace_realtime_updates,
    cleanup_old_realtime_updates,
)
from baserow.ws.tasks import (
    broadcast_to_channel_group,
    broadcast_to_group,
    broadcast_to_users,
    broadcast_to_users_individual_payloads,
    record_realtime_update,
)


@pytest.mark.django_db
@pytest.mark.websockets
def test_record_realtime_update_returns_increasing_ids():
    a = record_realtime_update(42, "ws-1")
    b = record_realtime_update(42, "ws-1")
    c = record_realtime_update(43, "ws-2")
    assert a < b
    assert c > b


@pytest.mark.django_db
@pytest.mark.websockets
def test_check_baseline_returns_no_updates_and_latest_id():
    record_realtime_update(101, "ws-a")
    last_id = record_realtime_update(101, "ws-b")
    has_updates, current_latest_id = check_workspace_realtime_updates(
        101, last_seen_id=None, previous_web_socket_id=None
    )
    assert has_updates is False
    assert current_latest_id == last_id


@pytest.mark.django_db
@pytest.mark.websockets
def test_check_workspace_with_no_rows_returns_zero_latest():
    has_updates, current_latest_id = check_workspace_realtime_updates(
        999999, last_seen_id=None, previous_web_socket_id=None
    )
    assert has_updates is False
    assert current_latest_id == 0


@pytest.mark.django_db
@pytest.mark.websockets
def test_check_filters_out_originator_session():
    # All rows are by "ws-self"; the client viewing as "ws-self" should not
    # be told there are updates because they caused them all.
    first_id = record_realtime_update(102, "ws-self")
    record_realtime_update(102, "ws-self")
    record_realtime_update(102, "ws-self")

    has_updates, _ = check_workspace_realtime_updates(
        102, last_seen_id=first_id, previous_web_socket_id="ws-self"
    )
    assert has_updates is False

    # A different originator added a row; now there are updates.
    record_realtime_update(102, "ws-other")
    has_updates, _ = check_workspace_realtime_updates(
        102, last_seen_id=first_id, previous_web_socket_id="ws-self"
    )
    assert has_updates is True


@pytest.mark.django_db
@pytest.mark.websockets
def test_check_treats_null_originator_as_someone_else():
    """``originator_session_id IS DISTINCT FROM`` ensures NULL-originator rows
    (system / Celery broadcasts) correctly count as someone-else changes."""

    first_id = record_realtime_update(103, "ws-self")
    record_realtime_update(103, None)
    has_updates, _ = check_workspace_realtime_updates(
        103, last_seen_id=first_id, previous_web_socket_id="ws-self"
    )
    assert has_updates is True


@pytest.mark.django_db
@pytest.mark.websockets
def test_check_ignores_other_workspaces():
    record_realtime_update(201, "ws-x")
    has_updates, current_latest_id = check_workspace_realtime_updates(
        202, last_seen_id=0, previous_web_socket_id=None
    )
    assert has_updates is False
    assert current_latest_id == 0


@pytest.mark.django_db
@pytest.mark.websockets
def test_cleanup_respects_per_workspace_limit():
    for _ in range(5):
        record_realtime_update(301, "ws-1")
    for _ in range(2):
        record_realtime_update(302, "ws-1")

    cleanup_old_realtime_updates(retention_hours=0, per_workspace_limit=2)

    # workspace 301 had 5 rows, only the 2 newest remain.
    _, latest_301 = check_workspace_realtime_updates(301, None, None)
    has_old_301, _ = check_workspace_realtime_updates(301, 0, None)
    assert has_old_301 is True
    # workspace 302 already at the limit, unaffected.
    _, latest_302 = check_workspace_realtime_updates(302, None, None)
    assert latest_302 > 0


@pytest.mark.django_db
@pytest.mark.websockets
def test_cleanup_respects_retention_hours():
    from django.db import connection

    # Insert an "old" row directly via SQL with a backdated created_at.
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO ws_realtime_updates "
            "(workspace_id, originator_session_id, created_at) "
            "VALUES (%s, %s, now() - interval '48 hours') RETURNING id",
            [501, "ws-old"],
        )
        old_id = cursor.fetchone()[0]

    new_id = record_realtime_update(501, "ws-new")

    by_age, _ = cleanup_old_realtime_updates(retention_hours=24, per_workspace_limit=0)
    assert by_age >= 1

    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM ws_realtime_updates WHERE workspace_id = 501")
        remaining_ids = [r[0] for r in cursor.fetchall()]
    assert old_id not in remaining_ids
    assert new_id in remaining_ids


@pytest.mark.django_db
@pytest.mark.websockets
def test_broadcast_to_group_records_row_and_injects_id(data_fixture):
    user = data_fixture.create_user()
    workspace = data_fixture.create_workspace(user=user)
    payload = {"type": "group_updated", "workspace_id": workspace.id}

    broadcast_to_group(workspace.id, payload)

    assert "realtime_update_id" in payload
    has_updates, current_latest = check_workspace_realtime_updates(
        workspace.id, last_seen_id=0, previous_web_socket_id=None
    )
    assert has_updates is True
    assert current_latest == payload["realtime_update_id"]


@pytest.mark.django_db
@pytest.mark.websockets
def test_broadcast_to_users_without_workspace_id_does_not_record(data_fixture):
    user = data_fixture.create_user()
    payload = {"type": "user_data_updated"}

    broadcast_to_users([user.id], payload)

    assert "realtime_update_id" not in payload


@pytest.mark.django_db
@pytest.mark.websockets
def test_broadcast_to_users_individual_payloads_shares_id_per_workspace(data_fixture):
    user_a = data_fixture.create_user()
    user_b = data_fixture.create_user()
    workspace = data_fixture.create_workspace(users=[user_a, user_b])

    payload_map = {
        str(user_a.id): {"type": "application_created", "workspace_id": workspace.id},
        str(user_b.id): {"type": "application_created", "workspace_id": workspace.id},
    }

    broadcast_to_users_individual_payloads(payload_map)

    id_a = payload_map[str(user_a.id)]["realtime_update_id"]
    id_b = payload_map[str(user_b.id)]["realtime_update_id"]
    assert id_a == id_b


@pytest.mark.django_db
@pytest.mark.websockets
def test_broadcast_to_channel_group_records_via_payload_workspace_id(data_fixture):
    workspace = data_fixture.create_workspace()
    payload = {"type": "something_happened", "workspace_id": workspace.id}

    broadcast_to_channel_group("dummy-group", payload)

    assert "realtime_update_id" in payload


@pytest.mark.django_db(transaction=True)
@pytest.mark.websockets
@pytest.mark.enable_all_signals
def test_table_page_type_broadcast_records_realtime_update(data_fixture):
    """row/field/view CRUD signals call ``table_page_type.broadcast`` with
    payloads that only carry ``table_id``. The PageType resolves workspace_id
    and passes it to the task so a row is recorded in ws_realtime_updates."""

    from baserow.contrib.database.ws.pages import (
        TablePageType,
        _workspace_id_for_table,
    )

    _workspace_id_for_table.cache_clear()

    user = data_fixture.create_user()
    workspace = data_fixture.create_workspace(user=user)
    database = data_fixture.create_database_application(workspace=workspace)
    table = data_fixture.create_database_table(database=database)

    payload = {"type": "rows_created", "table_id": table.id}
    TablePageType().broadcast(payload, None, table_id=table.id)

    assert "workspace_id" not in payload

    has_updates, latest = check_workspace_realtime_updates(
        workspace.id, last_seen_id=0, previous_web_socket_id=None
    )
    assert has_updates is True
    assert latest > 0


@pytest.mark.django_db(transaction=True)
@pytest.mark.websockets
@pytest.mark.enable_all_signals
def test_table_page_type_broadcast_uses_explicit_workspace_id_from_payload(
    data_fixture,
):
    from baserow.contrib.database.ws.pages import (
        TablePageType,
        _workspace_id_for_table,
    )

    _workspace_id_for_table.cache_clear()

    user = data_fixture.create_user()
    workspace = data_fixture.create_workspace(user=user)
    database = data_fixture.create_database_application(workspace=workspace)
    table = data_fixture.create_database_table(database=database)

    payload = {
        "type": "something",
        "table_id": table.id,
        "workspace_id": workspace.id,
    }
    TablePageType().broadcast(payload, None, table_id=table.id)

    has_updates, latest = check_workspace_realtime_updates(
        workspace.id, last_seen_id=0, previous_web_socket_id=None
    )
    assert has_updates is True
    assert latest > 0


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@pytest.mark.websockets
async def test_subscribe_baseline_returns_no_updates(data_fixture):
    user, token = await sync_to_async(data_fixture.create_user_and_token)()
    workspace = await sync_to_async(data_fixture.create_workspace)(user=user)

    communicator = WebsocketCommunicator(
        application,
        f"ws/core/?jwt_token={token}",
        headers=[(b"origin", b"http://localhost")],
    )
    connected, _ = await communicator.connect()
    assert connected is True
    await communicator.receive_json_from()

    await communicator.send_json_to(
        {
            "type": "workspace_realtime_subscribe",
            "workspace_id": workspace.id,
            "last_seen_id": None,
            "previous_web_socket_id": None,
        }
    )
    response = await communicator.receive_json_from(timeout=1)
    assert response["type"] == "workspace_realtime_subscribe_result"
    assert response["workspace_id"] == workspace.id
    assert response["has_updates"] is False
    assert response["current_latest_id"] == 0

    await communicator.disconnect()


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@pytest.mark.websockets
async def test_subscribe_after_other_updates_returns_has_updates(data_fixture):
    user, token = await sync_to_async(data_fixture.create_user_and_token)()
    workspace = await sync_to_async(data_fixture.create_workspace)(user=user)

    await sync_to_async(record_realtime_update)(workspace.id, "different-ws-id")

    communicator = WebsocketCommunicator(
        application,
        f"ws/core/?jwt_token={token}",
        headers=[(b"origin", b"http://localhost")],
    )
    connected, _ = await communicator.connect()
    assert connected is True
    await communicator.receive_json_from()

    await communicator.send_json_to(
        {
            "type": "workspace_realtime_subscribe",
            "workspace_id": workspace.id,
            "last_seen_id": 0,
            "previous_web_socket_id": "my-old-id",
        }
    )
    response = await communicator.receive_json_from(timeout=1)
    assert response["type"] == "workspace_realtime_subscribe_result"
    assert response["has_updates"] is True
    assert response["current_latest_id"] > 0

    await communicator.disconnect()


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@pytest.mark.websockets
async def test_subscribe_filters_out_own_session(data_fixture):
    user, token = await sync_to_async(data_fixture.create_user_and_token)()
    workspace = await sync_to_async(data_fixture.create_workspace)(user=user)

    await sync_to_async(record_realtime_update)(workspace.id, "my-old-id")

    communicator = WebsocketCommunicator(
        application,
        f"ws/core/?jwt_token={token}",
        headers=[(b"origin", b"http://localhost")],
    )
    connected, _ = await communicator.connect()
    assert connected is True
    await communicator.receive_json_from()

    await communicator.send_json_to(
        {
            "type": "workspace_realtime_subscribe",
            "workspace_id": workspace.id,
            "last_seen_id": 0,
            "previous_web_socket_id": "my-old-id",
        }
    )
    response = await communicator.receive_json_from(timeout=1)
    assert response["has_updates"] is False
    assert response["current_latest_id"] > 0

    await communicator.disconnect()


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@pytest.mark.websockets
async def test_subscribe_non_member_is_silently_dropped(data_fixture):
    user, token = await sync_to_async(data_fixture.create_user_and_token)()
    other_workspace = await sync_to_async(data_fixture.create_workspace)()
    await sync_to_async(record_realtime_update)(other_workspace.id, "x")

    communicator = WebsocketCommunicator(
        application,
        f"ws/core/?jwt_token={token}",
        headers=[(b"origin", b"http://localhost")],
    )
    connected, _ = await communicator.connect()
    assert connected is True
    await communicator.receive_json_from()

    await communicator.send_json_to(
        {
            "type": "workspace_realtime_subscribe",
            "workspace_id": other_workspace.id,
            "last_seen_id": 0,
            "previous_web_socket_id": None,
        }
    )
    assert await communicator.receive_nothing(timeout=0.5)

    await communicator.disconnect()


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@pytest.mark.websockets
async def test_subscribe_invalid_workspace_id_is_silently_dropped(data_fixture):
    user, token = await sync_to_async(data_fixture.create_user_and_token)()

    communicator = WebsocketCommunicator(
        application,
        f"ws/core/?jwt_token={token}",
        headers=[(b"origin", b"http://localhost")],
    )
    connected, _ = await communicator.connect()
    assert connected is True
    await communicator.receive_json_from()

    await communicator.send_json_to(
        {
            "type": "workspace_realtime_subscribe",
            "workspace_id": "not-an-int",
            "last_seen_id": 0,
            "previous_web_socket_id": None,
        }
    )
    assert await communicator.receive_nothing(timeout=0.5)

    await communicator.disconnect()
