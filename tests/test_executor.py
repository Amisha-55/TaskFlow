
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import time

from backend.app.database import SessionLocal
from backend.app.models.job import Job, JobStatus
from backend.app.worker.executor import (
    ExecutionStatus,
    execute_job,
)
from backend.app.worker.handlers import create_default_registry
import threading
from concurrent.futures import ThreadPoolExecutor

from backend.app.worker.handlers import HandlerRegistry
from backend.app.models.outbox import OutboxEvent
from backend.app.worker.recovery import recover_expired_leases
from backend.app.worker.lease import LeaseHeartbeat

def create_job(
    *,
    task_type="add",
    payload=None,
    scheduled_at=None,
    attempts=0,
    max_attempts=3,
):
    return Job(
        id=uuid4(),
        task_type=task_type,
        payload=payload if payload is not None else {"a": 2, "b": 3},
        status=JobStatus.PENDING,
        scheduled_at=scheduled_at,
        attempts=attempts,
        max_attempts=max_attempts,
    )


def test_execute_job_persists_successful_result():
    db = SessionLocal()
    job = create_job()
    job_id = job.id

    try:
        db.add(job)
        db.commit()

        outcome = execute_job(
            db, job_id, create_default_registry()
        )

        db.expire_all()
        saved = db.get(Job, job_id)

        assert outcome.status == ExecutionStatus.SUCCEEDED
        assert outcome.result == {"result": 5}
        assert saved is not None
        assert saved.status == JobStatus.SUCCEEDED
        assert saved.result == {"result": 5}
        assert saved.attempts == 1
        assert saved.started_at is not None
        assert saved.finished_at is not None
        assert saved.last_error is None
    finally:
        db.rollback()
        db.query(Job).filter(Job.id == job_id).delete()
        db.commit()
        db.close()


def test_execute_job_persists_handler_failure():
    db = SessionLocal()
    job = create_job(
        task_type="add",
        payload={"a": 2},
        max_attempts=1,
    )
    job_id = job.id

    try:
        db.add(job)
        db.commit()

        outcome = execute_job(
            db, job_id, create_default_registry()
        )

        db.expire_all()
        saved = db.get(Job, job_id)

        assert outcome.status == ExecutionStatus.FAILED
        assert outcome.error is not None
        assert "ValueError" in outcome.error
        assert saved is not None
        assert saved.status == JobStatus.FAILED
        assert saved.last_error == outcome.error
        assert saved.finished_at is not None
        assert saved.attempts == 1
    finally:
        db.rollback()
        db.query(Job).filter(Job.id == job_id).delete()
        db.commit()
        db.close()


def test_future_scheduled_job_is_not_claimed():
    db = SessionLocal()
    job = create_job(
        scheduled_at=datetime.now(timezone.utc) + timedelta(hours=1)
    )
    job_id = job.id

    try:
        db.add(job)
        db.commit()

        outcome = execute_job(
            db, job_id, create_default_registry()
        )

        db.expire_all()
        saved = db.get(Job, job_id)

        assert outcome.status == ExecutionStatus.NOT_CLAIMED
        assert saved is not None
        assert saved.status == JobStatus.PENDING
        assert saved.attempts == 0
        assert saved.started_at is None
    finally:
        db.rollback()
        db.query(Job).filter(Job.id == job_id).delete()
        db.commit()
        db.close()


def test_job_with_exhausted_attempts_is_not_claimed():
    db = SessionLocal()
    job = create_job(attempts=3, max_attempts=3)
    job_id = job.id

    try:
        db.add(job)
        db.commit()

        outcome = execute_job(
            db, job_id, create_default_registry()
        )

        db.expire_all()
        saved = db.get(Job, job_id)

        assert outcome.status == ExecutionStatus.NOT_CLAIMED
        assert saved is not None
        assert saved.status == JobStatus.PENDING
        assert saved.attempts == 3
    finally:
        db.rollback()
        db.query(Job).filter(Job.id == job_id).delete()
        db.commit()
        db.close()


def test_duplicate_execution_does_not_run_handler_twice():
    db = SessionLocal()
    job = create_job()
    job_id = job.id
    registry = create_default_registry()

    try:
        db.add(job)
        db.commit()

        first = execute_job(db, job_id, registry)
        second = execute_job(db, job_id, registry)

        db.expire_all()
        saved = db.get(Job, job_id)

        assert first.status == ExecutionStatus.SUCCEEDED
        assert second.status == ExecutionStatus.NOT_CLAIMED
        assert saved is not None
        assert saved.status == JobStatus.SUCCEEDED
        assert saved.attempts == 1
    finally:
        db.rollback()
        db.query(Job).filter(Job.id == job_id).delete()
        db.commit()
        db.close()


def test_concurrent_workers_execute_same_job_only_once():
    setup_db = SessionLocal()
    job = create_job(task_type="blocking")
    job_id = job.id

    handler_started = threading.Event()
    release_handler = threading.Event()
    execution_count = 0
    count_lock = threading.Lock()

    def blocking_handler(payload):
        nonlocal execution_count

        with count_lock:
            execution_count += 1

        handler_started.set()

        if not release_handler.wait(timeout=10):
            raise TimeoutError("Test did not release the handler.")

        return {"processed": True}

    registry = HandlerRegistry()
    registry.register("blocking", blocking_handler)

    try:
        setup_db.add(job)
        setup_db.commit()

        def run_worker():
            db = SessionLocal()
            try:
                return execute_job(db, job_id, registry)
            finally:
                db.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            first_future = pool.submit(run_worker)

            assert handler_started.wait(timeout=10), (
                "The first worker did not start its handler."
            )

            try:
                # The first worker is still executing, so the job
                # remains RUNNING. The second worker must not claim it.
                second_future = pool.submit(run_worker)
                second = second_future.result(timeout=10)

                assert second.status == ExecutionStatus.NOT_CLAIMED
            finally:
                release_handler.set()

            first = first_future.result(timeout=10)

        setup_db.expire_all()
        saved = setup_db.get(Job, job_id)

        assert first.status == ExecutionStatus.SUCCEEDED
        assert execution_count == 1
        assert saved is not None
        assert saved.status == JobStatus.SUCCEEDED
        assert saved.attempts == 1
        assert saved.result == {"processed": True}

    finally:
        release_handler.set()
        setup_db.rollback()
        setup_db.query(Job).filter(Job.id == job_id).delete()
        setup_db.commit()
        setup_db.close()


def test_handler_failure_schedules_retry():
    db = SessionLocal()
    job = create_job(
        task_type="add",
        payload={"a": 2},
        max_attempts=3,
    )
    job_id = job.id

    try:
        db.add(job)
        db.commit()

        outcome = execute_job(
            db, job_id, create_default_registry()
        )

        db.expire_all()
        saved = db.get(Job, job_id)

        assert outcome.status == ExecutionStatus.RETRY_SCHEDULED
        assert outcome.error is not None
        assert saved is not None
        assert saved.status == JobStatus.PENDING
        assert saved.attempts == 1
        assert saved.last_error == outcome.error
        assert saved.scheduled_at is not None
        assert saved.finished_at is None
    finally:
        db.rollback()
        db.query(Job).filter(Job.id == job_id).delete()
        db.commit()
        db.close()


def test_job_succeeds_after_one_retry():
    db = SessionLocal()
    job = create_job(
        task_type="flaky",
        payload={"value": 42},
        max_attempts=3,
    )
    job_id = job.id
    handler_calls = 0

    def flaky_handler(payload):
        nonlocal handler_calls
        handler_calls += 1

        if handler_calls == 1:
            raise ValueError("Temporary failure")

        return {"result": payload["value"]}

    registry = HandlerRegistry()
    registry.register("flaky", flaky_handler)

    try:
        db.add(job)
        db.commit()

        # First attempt fails and schedules a retry.
        first = execute_job(db, job_id, registry)

        db.expire_all()
        after_first = db.get(Job, job_id)

        assert first.status == ExecutionStatus.RETRY_SCHEDULED
        assert after_first is not None
        assert after_first.status == JobStatus.PENDING
        assert after_first.attempts == 1
        assert after_first.last_error == "ValueError: Temporary failure"
        assert after_first.scheduled_at is not None
        
        retry_event = (
            db.query(OutboxEvent)
            .filter(
                OutboxEvent.aggregate_id == job_id,
                OutboxEvent.event_type == "job.retry_scheduled",
            )
            .first()
        )

        assert retry_event is not None
        assert retry_event.payload["job_id"] == str(job_id)
        assert retry_event.payload["task_type"] == "flaky"
        assert retry_event.payload["enqueued_at"]
        # Make the scheduled retry immediately eligible for this test.
        after_first.scheduled_at = datetime.now(timezone.utc) - timedelta(
            seconds=1
        )
        db.commit()

        # Second attempt succeeds.
        second = execute_job(db, job_id, registry)

        db.expire_all()
        saved = db.get(Job, job_id)

        assert second.status == ExecutionStatus.SUCCEEDED
        assert second.result == {"result": 42}
        assert handler_calls == 2

        assert saved is not None
        assert saved.status == JobStatus.SUCCEEDED
        assert saved.attempts == 2
        assert saved.result == {"result": 42}
        assert saved.last_error is None
        assert saved.finished_at is not None

    finally:
        db.rollback()
        db.query(OutboxEvent).filter(
            OutboxEvent.aggregate_id == job_id,
            OutboxEvent.event_type == "job.retry_scheduled",
        ).delete(synchronize_session=False)
        db.query(Job).filter(Job.id == job_id).delete(
            synchronize_session=False
        )
        db.commit()
        db.close()

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

# Add these imports at the top if not already present:
# from backend.app.database import SessionLocal
# from backend.app.models.job import Job, JobStatus
# from backend.app.worker.executor import (
#     ExecutionStatus,
#     execute_job,
# )
# from backend.app.worker.handlers import HandlerRegistry
def test_only_one_worker_can_claim_a_job():
    setup_db = SessionLocal()
    job = create_job(
        task_type="echo",
        payload={"message": "concurrency-test"},
        max_attempts=3,
    )
    job_id = job.id

    try:
        setup_db.add(job)
        setup_db.commit()

        barrier = Barrier(2)

        def attempt_claim():
            db = SessionLocal()
            try:
                registry = HandlerRegistry()
                registry.register("echo", lambda payload: payload)
                barrier.wait(timeout=5) 
                return execute_job(db, job_id, registry)
            finally:
                db.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(attempt_claim),
                pool.submit(attempt_claim),
            ]
            results = [future.result(timeout=15) for future in futures]

        statuses = [result.status for result in results]

        assert statuses.count(ExecutionStatus.SUCCEEDED) == 1
        assert statuses.count(ExecutionStatus.NOT_CLAIMED) == 1

        setup_db.expire_all()
        saved = setup_db.get(Job, job_id)

        assert saved is not None
        assert saved.status == JobStatus.SUCCEEDED
        assert saved.attempts == 1

    finally:
        setup_db.rollback()
        setup_db.query(Job).filter(Job.id == job_id).delete(
            synchronize_session=False
        )
        setup_db.commit()
        setup_db.close()


def test_expired_lease_requeues_job_and_creates_outbox_event():
    db = SessionLocal()
    now = datetime.now(timezone.utc)
    job = create_job(attempts=1, max_attempts=3)
    job.status = JobStatus.RUNNING
    job.started_at = now - timedelta(minutes=2)
    job.claim_token = uuid4()
    job.lease_expires_at = now - timedelta(seconds=10)
    job_id = job.id

    try:
        db.add(job)
        db.commit()

        recovered = recover_expired_leases(
            db, now=now
        )

        db.expire_all()
        saved = db.get(Job, job_id)
        event = (
            db.query(OutboxEvent)
            .filter(
                OutboxEvent.aggregate_id == job_id,
                OutboxEvent.event_type == "job.retry_scheduled",
            )
            .first()
        )

        assert recovered == 1
        assert saved is not None
        assert saved.status == JobStatus.PENDING
        assert saved.attempts == 1
        assert saved.claim_token is None
        assert saved.lease_expires_at is None
        assert saved.scheduled_at > now
        assert saved.last_error is not None
        assert event is not None
        assert event.payload["job_id"] == str(job_id)

    finally:
        db.rollback()
        db.query(OutboxEvent).filter(
            OutboxEvent.aggregate_id == job_id
        ).delete(synchronize_session=False)
        db.query(Job).filter(
            Job.id == job_id
        ).delete(synchronize_session=False)
        db.commit()
        db.close()


def test_expired_lease_fails_job_when_attempts_exhausted():
    db = SessionLocal()
    now = datetime.now(timezone.utc)
    job = create_job(attempts=3, max_attempts=3)
    job.status = JobStatus.RUNNING
    job.started_at = now - timedelta(minutes=2)
    job.claim_token = uuid4()
    job.lease_expires_at = now - timedelta(seconds=10)
    job_id = job.id

    try:
        db.add(job)
        db.commit()

        recovered = recover_expired_leases(
            db, now=now
        )

        db.expire_all()
        saved = db.get(Job, job_id)

        assert recovered == 1
        assert saved is not None
        assert saved.status == JobStatus.FAILED
        assert saved.attempts == 3
        assert saved.finished_at is not None
        assert saved.claim_token is None
        assert saved.lease_expires_at is None

        retry_event = (
            db.query(OutboxEvent)
            .filter(
                OutboxEvent.aggregate_id == job_id,
                OutboxEvent.event_type == "job.retry_scheduled",
            )
            .first()
        )
        assert retry_event is None

    finally:
        db.rollback()
        db.query(OutboxEvent).filter(
            OutboxEvent.aggregate_id == job_id
        ).delete(synchronize_session=False)
        db.query(Job).filter(
            Job.id == job_id
        ).delete(synchronize_session=False)
        db.commit()
        db.close()


def test_stale_worker_cannot_finalize_after_lease_recovery():
    setup_db = SessionLocal()
    job = create_job(task_type="blocking", max_attempts=3)
    job_id = job.id

    handler_started = threading.Event()
    release_handler = threading.Event()

    def blocking_handler(payload):
        handler_started.set()
        if not release_handler.wait(timeout=10):
            raise TimeoutError("Test did not release the handler.")
        return {"processed": True}

    registry = HandlerRegistry()
    registry.register("blocking", blocking_handler)

    try:
        setup_db.add(job)
        setup_db.commit()

        def run_worker():
            worker_db = SessionLocal()
            try:
                return execute_job(worker_db, job_id, registry)
            finally:
                worker_db.close()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(run_worker)

            assert handler_started.wait(timeout=10)

            recovery_db = SessionLocal()
            try:
                running = recovery_db.get(Job, job_id)
                assert running is not None
                assert running.claim_token is not None

                # Simulate a lease expiring while the handler is running.
                running.lease_expires_at = (
                    datetime.now(timezone.utc) - timedelta(seconds=1)
                )
                recovery_db.commit()

                recovered = recover_expired_leases(recovery_db)
                assert recovered == 1
            finally:
                recovery_db.close()

            try:
                # Let the original, now-stale worker finish.
                release_handler.set()
                outcome = future.result(timeout=15)
            finally:
                release_handler.set()

        setup_db.expire_all()
        saved = setup_db.get(Job, job_id)

        assert outcome.status == ExecutionStatus.LEASE_LOST
        assert saved is not None
        assert saved.status == JobStatus.PENDING
        assert saved.attempts == 1
        assert saved.claim_token is None
        assert saved.lease_expires_at is None

    finally:
        release_handler.set()
        setup_db.rollback()
        setup_db.query(OutboxEvent).filter(
            OutboxEvent.aggregate_id == job_id
        ).delete(synchronize_session=False)
        setup_db.query(Job).filter(
            Job.id == job_id
        ).delete(synchronize_session=False)
        setup_db.commit()
        setup_db.close()

def test_lease_heartbeat_renews_active_lease(monkeypatch):
    monkeypatch.setattr(
        "backend.app.worker.lease.RENEW_INTERVAL_SECONDS",
        0.05,
    )

    db = SessionLocal()
    job = create_job()

    now = datetime.now(timezone.utc)
    claim_token = uuid4()
    original_expiry = now + timedelta(seconds=30)

    job.status = JobStatus.RUNNING
    job.claim_token = claim_token
    job.lease_expires_at = original_expiry
    job.updated_at = now

    db.add(job)
    db.commit()

    heartbeat = None

    try:
        heartbeat = LeaseHeartbeat(job.id, claim_token)
        heartbeat.start()

        # Allow the heartbeat to renew the lease.
        time.sleep(0.2)

        heartbeat.stop()
        heartbeat = None

        db.refresh(job)

        assert job.lease_expires_at > original_expiry
        assert job.claim_token == claim_token

    finally:
        if heartbeat is not None:
            heartbeat.stop()

        db.delete(job)
        db.commit()
        db.close()