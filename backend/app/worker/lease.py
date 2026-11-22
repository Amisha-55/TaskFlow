
import logging
import threading
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import update

from backend.app.database import SessionLocal
from backend.app.models.job import Job, JobStatus


logger = logging.getLogger(__name__)

# Renew well before the 60-second lease expires.
RENEW_INTERVAL_SECONDS = 15
LEASE_DURATION = timedelta(seconds=60)


class LeaseHeartbeat:
    def __init__(self, job_id: UUID, claim_token: UUID) -> None:
        self.job_id = job_id
        self.claim_token = claim_token
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"taskflow-lease-{job_id}",
            daemon=True,
        )
        self._lost = threading.Event()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()

    @property
    def lease_lost(self) -> bool:
        return self._lost.is_set()

    def _run(self) -> None:
        while not self._stop.wait(RENEW_INTERVAL_SECONDS):
            now = datetime.now(timezone.utc)

            try:
                with SessionLocal() as db:
                    result = db.execute(
                        update(Job)
                        .where(
                            Job.id == self.job_id,
                            Job.status == JobStatus.RUNNING,
                            Job.claim_token == self.claim_token,
                            Job.lease_expires_at > now,
                        )
                        .values(
                            lease_expires_at=now + LEASE_DURATION,
                            updated_at=now,
                        )
                        .execution_options(synchronize_session=False)
                    )
                    db.commit()

                    if result.rowcount != 1:
                        self._lost.set()
                        logger.warning(
                            "Lease lost for job %s",
                            self.job_id,
                        )
                        return

            except Exception:
                # Do not assume ownership after a failed renewal.
                self._lost.set()
                logger.exception(
                    "Lease renewal failed for job %s",
                    self.job_id,
                )
                return
