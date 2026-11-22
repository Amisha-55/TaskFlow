
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from backend.app.config import settings
from backend.app.database import SessionLocal
from backend.app.models.outbox import OutboxEvent
from backend.app.queue.outbox_relay import publish_pending_events


def test_postgres_skip_locked_allows_two_sessions_to_claim_different_events():
    first_id = uuid4()
    second_id = uuid4()
    now = datetime.now(timezone.utc)

    session_a = SessionLocal()
    session_b = SessionLocal()
    cleanup_session = SessionLocal()

    event_ids = [first_id, second_id]

    try:
        cleanup_session.add_all(
            [
                OutboxEvent(
                    id=first_id,
                    event_type="test.lock",
                    aggregate_id=uuid4(),
                    payload={"test_event": "first"},
                    created_at=now,
                ),
                OutboxEvent(
                    id=second_id,
                    event_type="test.lock",
                    aggregate_id=uuid4(),
                    payload={"test_event": "second"},
                    created_at=now + timedelta(seconds=1),
                ),
            ]
        )
        cleanup_session.commit()

        first_event = (
            session_a.query(OutboxEvent)
            .filter(
                OutboxEvent.id.in_(event_ids),
                OutboxEvent.published_at.is_(None),
            )
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .with_for_update(skip_locked=True)
            .first()
        )

        assert first_event is not None
        assert first_event.id == first_id

        second_event = (
            session_b.query(OutboxEvent)
            .filter(
                OutboxEvent.id.in_(event_ids),
                OutboxEvent.published_at.is_(None),
            )
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .with_for_update(skip_locked=True)
            .first()
        )

        assert second_event is not None
        assert second_event.id == second_id
        assert second_event.id != first_event.id

    finally:
        session_a.rollback()
        session_b.rollback()
        session_a.close()
        session_b.close()

        try:
            cleanup_session.query(OutboxEvent).filter(
                OutboxEvent.id.in_(event_ids)
            ).delete(synchronize_session=False)
            cleanup_session.commit()
        except Exception:
            cleanup_session.rollback()
            raise
        finally:
            cleanup_session.close()


def test_concurrent_relays_publish_different_events():
    # Use a unique schema so the real relay cannot consume unrelated
    # unpublished events from the development or test database.
    schema_name = f"taskflow_test_{uuid4().hex}"

    admin_engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
    )

    isolated_engine = None

    try:
        # The generated schema name contains only safe identifier characters.
        with admin_engine.begin() as connection:
            connection.execute(text(f"CREATE SCHEMA {schema_name}"))

        # Every connection created by this engine uses the isolated schema.
        isolated_engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            connect_args={
                "options": f"-csearch_path={schema_name},public"
            },
        )

        # The relay needs only the outbox table for this integration test.
        OutboxEvent.__table__.create(bind=isolated_engine)

        IsolatedSessionLocal = sessionmaker(
            bind=isolated_engine,
            autoflush=False,
            autocommit=False,
        )

        setup_session = IsolatedSessionLocal()
        first_id = uuid4()
        second_id = uuid4()
        now = datetime.now(timezone.utc)

        first_started = threading.Event()
        second_finished = threading.Event()
        published_ids = []
        published_lock = threading.Lock()

        try:
            setup_session.add_all(
                [
                    OutboxEvent(
                        id=first_id,
                        event_type="job.created",
                        aggregate_id=first_id,
                        payload={
                            "message_version": 1,
                            "job_id": str(first_id),
                            "task_type": "test_first",
                            "priority": 1,
                            "enqueued_at": now.isoformat(),
                        },
                        created_at=now,
                    ),
                    OutboxEvent(
                        id=second_id,
                        event_type="job.created",
                        aggregate_id=second_id,
                        payload={
                            "message_version": 1,
                            "job_id": str(second_id),
                            "task_type": "test_second",
                            "priority": 2,
                            "enqueued_at": (
                                now + timedelta(seconds=1)
                            ).isoformat(),
                        },
                        created_at=now + timedelta(seconds=1),
                    ),
                ]
            )
            setup_session.commit()

            class FirstPublisher:
                def publish(self, message):
                    with published_lock:
                        published_ids.append(message.job_id)

                    first_started.set()

                    if not second_finished.wait(timeout=10):
                        raise TimeoutError(
                            "The second relay did not finish in time."
                        )

            class SecondPublisher:
                def publish(self, message):
                    with published_lock:
                        published_ids.append(message.job_id)

            def run_first_relay():
                db = IsolatedSessionLocal()
                try:
                    return publish_pending_events(
                        db, FirstPublisher(), batch_size=10
                    )
                finally:
                    db.close()

            def run_second_relay():
                db = IsolatedSessionLocal()
                try:
                    return publish_pending_events(
                        db, SecondPublisher(), batch_size=10
                    )
                finally:
                    db.close()

            with ThreadPoolExecutor(max_workers=2) as executor:
                first_future = executor.submit(run_first_relay)

                assert first_started.wait(timeout=10), (
                    "The first relay did not begin publishing in time."
                )

                second_future = executor.submit(run_second_relay)

                try:
                    second_count = second_future.result(timeout=15)
                    assert second_count == 1
                finally:
                    second_finished.set()

                first_count = first_future.result(timeout=15)

            assert first_count == 1

            assert sorted(str(job_id) for job_id in published_ids) == sorted(
                [str(first_id), str(second_id)]
            )

            setup_session.expire_all()

            first_event = setup_session.get(OutboxEvent, first_id)
            second_event = setup_session.get(OutboxEvent, second_id)

            assert first_event is not None
            assert second_event is not None
            assert first_event.published_at is not None
            assert second_event.published_at is not None

        finally:
            setup_session.rollback()
            setup_session.close()

    finally:
        if isolated_engine is not None:
            isolated_engine.dispose()

        try:
            with admin_engine.begin() as connection:
                connection.execute(
                    text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                )
        finally:
            admin_engine.dispose()
