# 节点接口

## Base Job

在此接口的基础上，可以编辑相应所需接口。包括节点状态显示的相关字段信息，相关接口逻辑。

```python
import os
import signal
from typing import Optional
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job import BaseJob
from airflow.models.taskinstance import TaskInstance
from airflow.stats import Stats
from airflow.task.task_runner import get_task_runner
from airflow.utils import timezone
from airflow.utils.net import get_hostname
from airflow.utils.session import provide_session
from airflow.utils.state import State


class LocalTaskJob(BaseJob):
    """
    LocalTaskJob runs a single task instance.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance: TaskInstance,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            mark_success: bool = False,
            pickle_id: Optional[str] = None,
            pool: Optional[str] = None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.dag_id = task_instance.dag_id
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        self.task_runner = None

        # terminating state is used so that a job don't try to
        # terminate multiple times
        self.terminating = False
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
        super().__init__(*args, **kwargs)

    def _execute(self):
        self.task_runner = get_task_runner(self)

        def signal_handler(signum, frame):
            """Setting kill signal handler"""
            self.log.error("Received SIGTERM. Terminating subprocesses")
            self.on_kill()
            raise AirflowException("LocalTaskJob received SIGTERM signal")
        signal.signal(signal.SIGTERM, signal_handler)

        if not self.task_instance.check_and_change_state_before_execution(
                mark_success=self.mark_success,
                ignore_all_deps=self.ignore_all_deps,
                ignore_depends_on_past=self.ignore_depends_on_past,
                ignore_task_deps=self.ignore_task_deps,
                ignore_ti_state=self.ignore_ti_state,
                job_id=self.id,
                pool=self.pool):
            self.log.info("Task is not able to be run")
            return

        try:
            self.task_runner.start()

            heartbeat_time_limit = conf.getint('scheduler',
                                               'scheduler_zombie_task_threshold')
            while True:
                # Monitor the task to see if it's done
                return_code = self.task_runner.return_code()
                if return_code is not None:
                    self.log.info("Task exited with return code %s", return_code)
                    return

                self.heartbeat()

                # If it's been too long since we've heartbeat, then it's possible that
                # the scheduler rescheduled this task, so kill launched processes.
                # This can only really happen if the worker can't read the DB for a long time
                time_since_last_heartbeat = (timezone.utcnow() - self.latest_heartbeat).total_seconds()
                if time_since_last_heartbeat > heartbeat_time_limit:
                    Stats.incr('local_task_job_prolonged_heartbeat_failure', 1, 1)
                    self.log.error("Heartbeat time limit exceeded!")
                    raise AirflowException("Time since last heartbeat({:.2f}s) "
                                           "exceeded limit ({}s)."
                                           .format(time_since_last_heartbeat,
                                                   heartbeat_time_limit))
        finally:
            self.on_kill()

    def on_kill(self):
        self.task_runner.terminate()
        self.task_runner.on_finish()

    @provide_session
    def heartbeat_callback(self, session=None):
        """Self destruct task if state has been moved away from running externally"""

        if self.terminating:
            # ensure termination if processes are created later
            self.task_runner.terminate()
            return

        self.task_instance.refresh_from_db()
        ti = self.task_instance

        if ti.state == State.RUNNING:
            fqdn = get_hostname()
            same_hostname = fqdn == ti.hostname
            if not same_hostname:
                self.log.warning("The recorded hostname %s "
                                 "does not match this instance's hostname "
                                 "%s", ti.hostname, fqdn)
                raise AirflowException("Hostname of job runner does not match")

            current_pid = os.getpid()
            same_process = ti.pid == current_pid
            if not same_process:
                self.log.warning("Recorded pid %s does not match "
                                 "the current pid %s", ti.pid, current_pid)
                raise AirflowException("PID of job runner does not match")
        elif (
                self.task_runner.return_code() is None and
                hasattr(self.task_runner, 'process')
        ):
            self.log.warning(
                "State of this instance has been externally set to %s. "
                "Taking the poison pill.",
                ti.state
            )
            if ti.state == State.FAILED and ti.task.on_failure_callback:
                context = ti.get_template_context()
                ti.task.on_failure_callback(context)
            if ti.state == State.SUCCESS and ti.task.on_success_callback:
                context = ti.get_template_context()
                ti.task.on_success_callback(context)
            self.task_runner.terminate()
            self.terminating = True

```

## Backfill Job

回填作业由特定时间范围内的dag或subdag组成。它以正确的顺序触发一组任务实例运行，并持续到完成一组任务实例所需的时间。

```python
import time
from collections import OrderedDict
from datetime import datetime
from typing import Set

from sqlalchemy.orm.session import Session, make_transient
from tabulate import tabulate

from airflow import models
from airflow.exceptions import (
    AirflowException, BackfillUnfinished, DagConcurrencyLimitReached, NoAvailablePoolSlot, PoolNotFound,
    TaskConcurrencyLimitReached,
)
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagPickle
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceKeyType
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import BACKFILL_QUEUED_DEPS
from airflow.utils import timezone
from airflow.utils.configuration import tmp_configuration_copy
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType


class BackfillJob(BaseJob):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """
    STATES_COUNT_AS_RUNNING = (State.RUNNING, State.QUEUED)

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    class _DagRunTaskStatus:
        """
        Internal status of the backfill job. This class is intended to be instantiated
        only within a BackfillJob instance and will track the execution of tasks,
        e.g. running, skipped, succeeded, failed, etc. Information about the dag runs
        related to the backfill job are also being tracked in this structure,
        .e.g finished runs, etc. Any other status related information related to the
        execution of dag runs / tasks can be included in this structure since it makes
        it easier to pass it around.

        :param to_run: Tasks to run in the backfill
        :type to_run: dict[tuple[TaskInstanceKeyType], airflow.models.TaskInstance]
        :param running: Maps running task instance key to task instance object
        :type running: dict[tuple[TaskInstanceKeyType], airflow.models.TaskInstance]
        :param skipped: Tasks that have been skipped
        :type skipped: set[tuple[TaskInstanceKeyType]]
        :param succeeded: Tasks that have succeeded so far
        :type succeeded: set[tuple[TaskInstanceKeyType]]
        :param failed: Tasks that have failed
        :type failed: set[tuple[TaskInstanceKeyType]]
        :param not_ready: Tasks not ready for execution
        :type not_ready: set[tuple[TaskInstanceKeyType]]
        :param deadlocked: Deadlocked tasks
        :type deadlocked: set[airflow.models.TaskInstance]
        :param active_runs: Active dag runs at a certain point in time
        :type active_runs: list[DagRun]
        :param executed_dag_run_dates: Datetime objects for the executed dag runs
        :type executed_dag_run_dates: set[datetime.datetime]
        :param finished_runs: Number of finished runs so far
        :type finished_runs: int
        :param total_runs: Number of total dag runs able to run
        :type total_runs: int
        """
        # TODO(edgarRd): AIRFLOW-1444: Add consistency check on counts
        def __init__(self,  # pylint: disable=too-many-arguments
                     to_run=None,
                     running=None,
                     skipped=None,
                     succeeded=None,
                     failed=None,
                     not_ready=None,
                     deadlocked=None,
                     active_runs=None,
                     executed_dag_run_dates=None,
                     finished_runs=0,
                     total_runs=0,
                     ):
            self.to_run = to_run or OrderedDict()
            self.running = running or dict()
            self.skipped = skipped or set()
            self.succeeded = succeeded or set()
            self.failed = failed or set()
            self.not_ready = not_ready or set()
            self.deadlocked = deadlocked or set()
            self.active_runs = active_runs or []
            self.executed_dag_run_dates = executed_dag_run_dates or set()
            self.finished_runs = finished_runs
            self.total_runs = total_runs

    def __init__(  # pylint: disable=too-many-arguments
            self,
            dag,
            start_date=None,
            end_date=None,
            mark_success=False,
            donot_pickle=False,
            ignore_first_depends_on_past=False,
            ignore_task_deps=False,
            pool=None,
            delay_on_limit_secs=1.0,
            verbose=False,
            conf=None,
            rerun_failed_tasks=False,
            run_backwards=False,
            *args, **kwargs):
        """
        :param dag: DAG object.
        :type dag: airflow.models.DAG
        :param start_date: start date for the backfill date range.
        :type start_date: datetime.datetime
        :param end_date: end date for the backfill date range.
        :type end_date: datetime.datetime
        :param mark_success: flag whether to mark the task auto success.
        :type mark_success: bool
        :param donot_pickle: whether pickle
        :type donot_pickle: bool
        :param ignore_first_depends_on_past: whether to ignore depend on past
        :type ignore_first_depends_on_past: bool
        :param ignore_task_deps: whether to ignore the task dependency
        :type ignore_task_deps: bool
        :param pool: pool to backfill
        :type pool: str
        :param delay_on_limit_secs:
        :param verbose:
        :type verbose: flag to whether display verbose message to backfill console
        :param conf: a dictionary which user could pass k-v pairs for backfill
        :type conf: dictionary
        :param rerun_failed_tasks: flag to whether to
                                   auto rerun the failed task in backfill
        :type rerun_failed_tasks: bool
        :param run_backwards: Whether to process the dates from most to least recent
        :type run_backwards bool
        :param args:
        :param kwargs:
        """
        self.dag = dag
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.donot_pickle = donot_pickle
        self.ignore_first_depends_on_past = ignore_first_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.pool = pool
        self.delay_on_limit_secs = delay_on_limit_secs
        self.verbose = verbose
        self.conf = conf
        self.rerun_failed_tasks = rerun_failed_tasks
        self.run_backwards = run_backwards
        super().__init__(*args, **kwargs)

    @provide_session
    def _update_counters(self, ti_status, session=None):
        """
        Updates the counters per state of the tasks that were running. Can re-add
        to tasks to run in case required.

        :param ti_status: the internal status of the backfill job tasks
        :type ti_status: BackfillJob._DagRunTaskStatus
        """
        tis_to_be_scheduled = []
        refreshed_tis = []
        TI = TaskInstance

        filter_for_tis = TI.filter_for_tis(list(ti_status.running.values()))
        if filter_for_tis is not None:
            refreshed_tis = session.query(TI).filter(filter_for_tis).all()

        for ti in refreshed_tis:
            # Here we remake the key by subtracting 1 to match in memory information
            key = (ti.dag_id, ti.task_id, ti.execution_date, max(1, ti.try_number - 1))
            if ti.state == State.SUCCESS:
                ti_status.succeeded.add(key)
                self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                ti_status.running.pop(key)
                continue
            if ti.state == State.SKIPPED:
                ti_status.skipped.add(key)
                self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                ti_status.running.pop(key)
                continue
            if ti.state == State.FAILED:
                self.log.error("Task instance %s failed", ti)
                ti_status.failed.add(key)
                ti_status.running.pop(key)
                continue
            # special case: if the task needs to run again put it back
            if ti.state == State.UP_FOR_RETRY:
                self.log.warning("Task instance %s is up for retry", ti)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti
            # special case: if the task needs to be rescheduled put it back
            elif ti.state == State.UP_FOR_RESCHEDULE:
                self.log.warning("Task instance %s is up for reschedule", ti)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti
            # special case: The state of the task can be set to NONE by the task itself
            # when it reaches concurrency limits. It could also happen when the state
            # is changed externally, e.g. by clearing tasks from the ui. We need to cover
            # for that as otherwise those tasks would fall outside of the scope of
            # the backfill suddenly.
            elif ti.state == State.NONE:
                self.log.warning(
                    "FIXME: task instance %s state was set to none externally or "
                    "reaching concurrency limits. Re-adding task to queue.",
                    ti
                )
                tis_to_be_scheduled.append(ti)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti

        # Batch schedule of task instances
        if tis_to_be_scheduled:
            filter_for_tis = TI.filter_for_tis(tis_to_be_scheduled)
            session.query(TI).filter(filter_for_tis).update(
                values={TI.state: State.SCHEDULED}, synchronize_session=False
            )

    def _manage_executor_state(self, running):
        """
        Checks if the executor agrees with the state of task instances
        that are running

        :param running: dict of key, task to verify
        """
        executor = self.executor

        # TODO: query all instead of refresh from db
        for key, state in list(executor.get_event_buffer().items()):
            if key not in running:
                self.log.warning(
                    "%s state %s not in running=%s",
                    key, state, running.values()
                )
                continue

            ti = running[key]
            ti.refresh_from_db()

            self.log.debug("Executor state: %s task %s", state, ti)

            if state in (State.FAILED, State.SUCCESS):
                if ti.state == State.RUNNING or ti.state == State.QUEUED:
                    msg = ("Executor reports task instance {} finished ({}) "
                           "although the task says its {}. Was the task "
                           "killed externally?".format(ti, state, ti.state))
                    self.log.error(msg)
                    ti.handle_failure(msg)

    @provide_session
    def _get_dag_run(self, run_date: datetime, dag: DAG, session: Session = None):
        """
        Returns a dag run for the given run date, which will be matched to an existing
        dag run if available or create a new dag run otherwise. If the max_active_runs
        limit is reached, this function will return None.

        :param run_date: the execution date for the dag run
        :param dag: DAG
        :param session: the database session object
        :return: a DagRun in state RUNNING or None
        """
        run_id = f"{DagRunType.BACKFILL_JOB.value}__{run_date.isoformat()}"

        # consider max_active_runs but ignore when running subdags
        respect_dag_max_active_limit = bool(dag.schedule_interval and not dag.is_subdag)

        current_active_dag_count = dag.get_num_active_runs(external_trigger=False)

        # check if we are scheduling on top of a already existing dag_run
        # we could find a "scheduled" run instead of a "backfill"
        run = DagRun.find(dag_id=dag.dag_id,
                          execution_date=run_date,
                          session=session)

        if run is not None and len(run) > 0:
            run = run[0]
            if run.state == State.RUNNING:
                respect_dag_max_active_limit = False
        else:
            run = None

        # enforce max_active_runs limit for dag, special cases already
        # handled by respect_dag_max_active_limit
        if (respect_dag_max_active_limit and
                current_active_dag_count >= dag.max_active_runs):
            return None

        run = run or dag.create_dagrun(
            run_id=run_id,
            execution_date=run_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            external_trigger=False,
            session=session,
            conf=self.conf,
        )

        # set required transient field
        run.dag = dag

        # explicitly mark as backfill and running
        run.state = State.RUNNING
        run.run_id = run_id
        run.verify_integrity(session=session)
        return run

    @provide_session
    def _task_instances_for_dag_run(self, dag_run, session=None):
        """
        Returns a map of task instance key to task instance object for the tasks to
        run in the given dag run.

        :param dag_run: the dag run to get the tasks from
        :type dag_run: airflow.models.DagRun
        :param session: the database session object
        :type session: sqlalchemy.orm.session.Session
        """
        tasks_to_run = {}

        if dag_run is None:
            return tasks_to_run

        # check if we have orphaned tasks
        self.reset_state_for_orphaned_tasks(filter_by_dag_run=dag_run, session=session)

        # for some reason if we don't refresh the reference to run is lost
        dag_run.refresh_from_db()
        make_transient(dag_run)

        try:
            for ti in dag_run.get_task_instances():
                # all tasks part of the backfill are scheduled to run
                if ti.state == State.NONE:
                    ti.set_state(State.SCHEDULED, session=session, commit=False)
                if ti.state != State.REMOVED:
                    tasks_to_run[ti.key] = ti
            session.commit()
        except Exception:
            session.rollback()
            raise

        return tasks_to_run

    def _log_progress(self, ti_status):
        self.log.info(
            '[backfill progress] | finished run %s of %s | tasks waiting: %s | succeeded: %s | '
            'running: %s | failed: %s | skipped: %s | deadlocked: %s | not ready: %s',
            ti_status.finished_runs, ti_status.total_runs, len(ti_status.to_run), len(ti_status.succeeded),
            len(ti_status.running), len(ti_status.failed), len(ti_status.skipped), len(ti_status.deadlocked),
            len(ti_status.not_ready)
        )

        self.log.debug(
            "Finished dag run loop iteration. Remaining tasks %s",
            ti_status.to_run.values()
        )

    @provide_session
    def _process_backfill_task_instances(self,  # pylint: disable=too-many-statements
                                         ti_status,
                                         executor,
                                         pickle_id,
                                         start_date=None, session=None):
        """
        Process a set of task instances from a set of dag runs. Special handling is done
        to account for different task instance states that could be present when running
        them in a backfill process.

        :param ti_status: the internal status of the job
        :type ti_status: BackfillJob._DagRunTaskStatus
        :param executor: the executor to run the task instances
        :type executor: BaseExecutor
        :param pickle_id: the pickle_id if dag is pickled, None otherwise
        :type pickle_id: int
        :param start_date: the start date of the backfill job
        :type start_date: datetime.datetime
        :param session: the current session object
        :type session: sqlalchemy.orm.session.Session
        :return: the list of execution_dates for the finished dag runs
        :rtype: list
        """

        executed_run_dates = []

        while ((len(ti_status.to_run) > 0 or len(ti_status.running) > 0) and
                len(ti_status.deadlocked) == 0):
            self.log.debug("*** Clearing out not_ready list ***")
            ti_status.not_ready.clear()

            # we need to execute the tasks bottom to top
            # or leaf to root, as otherwise tasks might be
            # determined deadlocked while they are actually
            # waiting for their upstream to finish
            @provide_session
            def _per_task_process(task, key, ti, session=None):  # pylint: disable=too-many-return-statements
                ti.refresh_from_db(lock_for_update=True, session=session)

                task = self.dag.get_task(ti.task_id, include_subdags=True)
                ti.task = task

                ignore_depends_on_past = (
                    self.ignore_first_depends_on_past and
                    ti.execution_date == (start_date or ti.start_date))
                self.log.debug(
                    "Task instance to run %s state %s", ti, ti.state)

                # The task was already marked successful or skipped by a
                # different Job. Don't rerun it.
                if ti.state == State.SUCCESS:
                    ti_status.succeeded.add(key)
                    self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return
                elif ti.state == State.SKIPPED:
                    ti_status.skipped.add(key)
                    self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return

                # guard against externally modified tasks instances or
                # in case max concurrency has been reached at task runtime
                elif ti.state == State.NONE:
                    self.log.warning(
                        "FIXME: task instance {} state was set to None "
                        "externally. This should not happen"
                    )
                    ti.set_state(State.SCHEDULED, session=session)
                if self.rerun_failed_tasks:
                    # Rerun failed tasks or upstreamed failed tasks
                    if ti.state in (State.FAILED, State.UPSTREAM_FAILED):
                        self.log.error("Task instance %s with state %s", ti, ti.state)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        # Reset the failed task in backfill to scheduled state
                        ti.set_state(State.SCHEDULED, session=session)
                else:
                    # Default behaviour which works for subdag.
                    if ti.state in (State.FAILED, State.UPSTREAM_FAILED):
                        self.log.error("Task instance %s with state %s", ti, ti.state)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        return

                backfill_context = DepContext(
                    deps=BACKFILL_QUEUED_DEPS,
                    ignore_depends_on_past=ignore_depends_on_past,
                    ignore_task_deps=self.ignore_task_deps,
                    flag_upstream_failed=True)

                # Is the task runnable? -- then run it
                # the dependency checker can change states of tis
                if ti.are_dependencies_met(
                        dep_context=backfill_context,
                        session=session,
                        verbose=self.verbose):
                    if executor.has_task(ti):
                        self.log.debug(
                            "Task Instance %s already in executor "
                            "waiting for queue to clear",
                            ti
                        )
                    else:
                        self.log.debug('Sending %s to executor', ti)
                        # Skip scheduled state, we are executing immediately
                        ti.state = State.QUEUED
                        ti.queued_dttm = timezone.utcnow()
                        session.merge(ti)

                        cfg_path = None
                        if executor.__class__ in (LocalExecutor, SequentialExecutor):
                            cfg_path = tmp_configuration_copy()

                        executor.queue_task_instance(
                            ti,
                            mark_success=self.mark_success,
                            pickle_id=pickle_id,
                            ignore_task_deps=self.ignore_task_deps,
                            ignore_depends_on_past=ignore_depends_on_past,
                            pool=self.pool,
                            cfg_path=cfg_path)
                        ti_status.running[key] = ti
                        ti_status.to_run.pop(key)
                    session.commit()
                    return

                if ti.state == State.UPSTREAM_FAILED:
                    self.log.error("Task instance %s upstream failed", ti)
                    ti_status.failed.add(key)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return

                # special case
                if ti.state == State.UP_FOR_RETRY:
                    self.log.debug(
                        "Task instance %s retry period not "
                        "expired yet", ti)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    ti_status.to_run[key] = ti
                    return

                # special case
                if ti.state == State.UP_FOR_RESCHEDULE:
                    self.log.debug(
                        "Task instance %s reschedule period not "
                        "expired yet", ti)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    ti_status.to_run[key] = ti
                    return

                # all remaining tasks
                self.log.debug('Adding %s to not_ready', ti)
                ti_status.not_ready.add(key)

            try:  # pylint: disable=too-many-nested-blocks
                for task in self.dag.topological_sort(include_subdag_tasks=True):
                    for key, ti in list(ti_status.to_run.items()):
                        if task.task_id != ti.task_id:
                            continue

                        pool = session.query(models.Pool) \
                            .filter(models.Pool.pool == task.pool) \
                            .first()
                        if not pool:
                            raise PoolNotFound('Unknown pool: {}'.format(task.pool))

                        open_slots = pool.open_slots(session=session)
                        if open_slots <= 0:
                            raise NoAvailablePoolSlot(
                                "Not scheduling since there are "
                                "{0} open slots in pool {1}".format(
                                    open_slots, task.pool))

                        num_running_task_instances_in_dag = DAG.get_num_task_instances(
                            self.dag_id,
                            states=self.STATES_COUNT_AS_RUNNING,
                        )

                        if num_running_task_instances_in_dag >= self.dag.concurrency:
                            raise DagConcurrencyLimitReached(
                                "Not scheduling since DAG concurrency limit "
                                "is reached."
                            )

                        if task.task_concurrency:
                            num_running_task_instances_in_task = DAG.get_num_task_instances(
                                dag_id=self.dag_id,
                                task_ids=[task.task_id],
                                states=self.STATES_COUNT_AS_RUNNING,
                            )

                            if num_running_task_instances_in_task >= task.task_concurrency:
                                raise TaskConcurrencyLimitReached(
                                    "Not scheduling since Task concurrency limit "
                                    "is reached."
                                )

                        _per_task_process(task, key, ti)
            except (NoAvailablePoolSlot, DagConcurrencyLimitReached, TaskConcurrencyLimitReached) as e:
                self.log.debug(e)

            # execute the tasks in the queue
            self.heartbeat()
            executor.heartbeat()

            # If the set of tasks that aren't ready ever equals the set of
            # tasks to run and there are no running tasks then the backfill
            # is deadlocked
            if (ti_status.not_ready and
                    ti_status.not_ready == set(ti_status.to_run) and
                    len(ti_status.running) == 0):
                self.log.warning(
                    "Deadlock discovered for ti_status.to_run=%s",
                    ti_status.to_run.values()
                )
                ti_status.deadlocked.update(ti_status.to_run.values())
                ti_status.to_run.clear()

            # check executor state
            self._manage_executor_state(ti_status.running)

            # update the task counters
            self._update_counters(ti_status=ti_status)

            # update dag run state
            _dag_runs = ti_status.active_runs[:]
            for run in _dag_runs:
                run.update_state(session=session)
                if run.state in State.finished():
                    ti_status.finished_runs += 1
                    ti_status.active_runs.remove(run)
                    executed_run_dates.append(run.execution_date)

            self._log_progress(ti_status)

        # return updated status
        return executed_run_dates

    @provide_session
    def _collect_errors(self, ti_status, session=None):
        def tabulate_ti_keys_set(set_ti_keys: Set[TaskInstanceKeyType]) -> str:
            # Sorting by execution date first
            sorted_ti_keys = sorted(
                set_ti_keys, key=lambda ti_key: (ti_key[2], ti_key[0], ti_key[1], ti_key[3]))
            return tabulate(sorted_ti_keys, headers=["DAG ID", "Task ID", "Execution date", "Try number"])

        def tabulate_tis_set(set_tis: Set[TaskInstance]) -> str:
            # Sorting by execution date first
            sorted_tis = sorted(
                set_tis, key=lambda ti: (ti.execution_date, ti.dag_id, ti.task_id, ti.try_number))
            tis_values = (
                (ti.dag_id, ti.task_id, ti.execution_date, ti.try_number)
                for ti in sorted_tis
            )
            return tabulate(tis_values, headers=["DAG ID", "Task ID", "Execution date", "Try number"])

        err = ''
        if ti_status.failed:
            err += "Some task instances failed:\n"
            err += tabulate_ti_keys_set(ti_status.failed)
        if ti_status.deadlocked:
            err += 'BackfillJob is deadlocked.'
            deadlocked_depends_on_past = any(
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=False),
                    session=session,
                    verbose=self.verbose) !=
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=True),
                    session=session,
                    verbose=self.verbose)
                for t in ti_status.deadlocked)
            if deadlocked_depends_on_past:
                err += (
                    'Some of the deadlocked tasks were unable to run because '
                    'of "depends_on_past" relationships. Try running the '
                    'backfill with the option '
                    '"ignore_first_depends_on_past=True" or passing "-I" at '
                    'the command line.')
            err += '\nThese tasks have succeeded:\n'
            err += tabulate_ti_keys_set(ti_status.succeeded)
            err += '\n\nThese tasks are running:\n'
            err += tabulate_ti_keys_set(ti_status.running)
            err += '\n\nThese tasks have failed:\n'
            err += tabulate_ti_keys_set(ti_status.failed)
            err += '\n\nThese tasks are skipped:\n'
            err += tabulate_ti_keys_set(ti_status.skipped)
            err += '\n\nThese tasks are deadlocked:\n'
            err += tabulate_tis_set(ti_status.deadlocked)

        return err

    @provide_session
    def _execute_for_run_dates(self, run_dates, ti_status, executor, pickle_id,
                               start_date, session=None):
        """
        Computes the dag runs and their respective task instances for
        the given run dates and executes the task instances.
        Returns a list of execution dates of the dag runs that were executed.

        :param run_dates: Execution dates for dag runs
        :type run_dates: list
        :param ti_status: internal BackfillJob status structure to tis track progress
        :type ti_status: BackfillJob._DagRunTaskStatus
        :param executor: the executor to use, it must be previously started
        :type executor: BaseExecutor
        :param pickle_id: numeric id of the pickled dag, None if not pickled
        :type pickle_id: int
        :param start_date: backfill start date
        :type start_date: datetime.datetime
        :param session: the current session object
        :type session: sqlalchemy.orm.session.Session
        """
        for next_run_date in run_dates:
            for dag in [self.dag] + self.dag.subdags:
                dag_run = self._get_dag_run(next_run_date, dag, session=session)
                tis_map = self._task_instances_for_dag_run(dag_run,
                                                           session=session)
                if dag_run is None:
                    continue

                ti_status.active_runs.append(dag_run)
                ti_status.to_run.update(tis_map or {})

        processed_dag_run_dates = self._process_backfill_task_instances(
            ti_status=ti_status,
            executor=executor,
            pickle_id=pickle_id,
            start_date=start_date,
            session=session)

        ti_status.executed_dag_run_dates.update(processed_dag_run_dates)

    @provide_session
    def _set_unfinished_dag_runs_to_failed(self, dag_runs, session=None):
        """
        Go through the dag_runs and update the state based on the task_instance state.
        Then set DAG runs that are not finished to failed.

        :param dag_runs: DAG runs
        :param session: session
        :return: None
        """
        for dag_run in dag_runs:
            dag_run.update_state()
            if dag_run.state not in State.finished():
                dag_run.set_state(State.FAILED)
            session.merge(dag_run)

    @provide_session
    def _execute(self, session=None):
        """
        Initializes all components required to run a dag for a specified date range and
        calls helper method to execute the tasks.
        """
        ti_status = BackfillJob._DagRunTaskStatus()

        start_date = self.bf_start_date

        # Get intervals between the start/end dates, which will turn into dag runs
        run_dates = self.dag.get_run_dates(start_date=start_date,
                                           end_date=self.bf_end_date)
        if self.run_backwards:
            tasks_that_depend_on_past = [t.task_id for t in self.dag.task_dict.values() if t.depends_on_past]
            if tasks_that_depend_on_past:
                raise AirflowException(
                    'You cannot backfill backwards because one or more tasks depend_on_past: {}'.format(
                        ",".join(tasks_that_depend_on_past)))
            run_dates = run_dates[::-1]

        if len(run_dates) == 0:
            self.log.info("No run dates were found for the given dates and dag interval.")
            return

        # picklin'
        pickle_id = None

        try:
            from airflow.executors.dask_executor import DaskExecutor
        except ImportError:
            DaskExecutor = None

        if not self.donot_pickle and \
                self.executor.__class__ not in (LocalExecutor, SequentialExecutor, DaskExecutor):
            pickle = DagPickle(self.dag)
            session.add(pickle)
            session.commit()
            pickle_id = pickle.id

        executor = self.executor
        executor.start()

        ti_status.total_runs = len(run_dates)  # total dag runs in backfill

        try:  # pylint: disable=too-many-nested-blocks
            remaining_dates = ti_status.total_runs
            while remaining_dates > 0:
                dates_to_process = [run_date for run_date in run_dates if run_date not in
                                    ti_status.executed_dag_run_dates]

                self._execute_for_run_dates(run_dates=dates_to_process,
                                            ti_status=ti_status,
                                            executor=executor,
                                            pickle_id=pickle_id,
                                            start_date=start_date,
                                            session=session)

                remaining_dates = (
                    ti_status.total_runs - len(ti_status.executed_dag_run_dates)
                )
                err = self._collect_errors(ti_status=ti_status, session=session)
                if err:
                    raise BackfillUnfinished(err, ti_status)

                if remaining_dates > 0:
                    self.log.info(
                        "max_active_runs limit for dag %s has been reached "
                        " - waiting for other dag runs to finish",
                        self.dag_id
                    )
                    time.sleep(self.delay_on_limit_secs)
        except (KeyboardInterrupt, SystemExit):
            self.log.warning("Backfill terminated by user.")

            # TODO: we will need to terminate running task instances and set the
            # state to failed.
            self._set_unfinished_dag_runs_to_failed(ti_status.active_runs)
        finally:
            session.commit()
            executor.end()

        self.log.info("Backfill done. Exiting.")
```

## Local Task Job

Local Task Job运行单个任务实例。

```python
import os
import signal
from typing import Optional

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job import BaseJob
from airflow.models.taskinstance import TaskInstance
from airflow.stats import Stats
from airflow.task.task_runner import get_task_runner
from airflow.utils import timezone
from airflow.utils.net import get_hostname
from airflow.utils.session import provide_session
from airflow.utils.state import State


class LocalTaskJob(BaseJob):
    """
    LocalTaskJob runs a single task instance.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance: TaskInstance,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            mark_success: bool = False,
            pickle_id: Optional[str] = None,
            pool: Optional[str] = None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.dag_id = task_instance.dag_id
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        self.task_runner = None

        # terminating state is used so that a job don't try to
        # terminate multiple times
        self.terminating = False

        super().__init__(*args, **kwargs)

    def _execute(self):
        self.task_runner = get_task_runner(self)

        def signal_handler(signum, frame):
            """Setting kill signal handler"""
            self.log.error("Received SIGTERM. Terminating subprocesses")
            self.on_kill()
            raise AirflowException("LocalTaskJob received SIGTERM signal")
        signal.signal(signal.SIGTERM, signal_handler)

        if not self.task_instance.check_and_change_state_before_execution(
                mark_success=self.mark_success,
                ignore_all_deps=self.ignore_all_deps,
                ignore_depends_on_past=self.ignore_depends_on_past,
                ignore_task_deps=self.ignore_task_deps,
                ignore_ti_state=self.ignore_ti_state,
                job_id=self.id,
                pool=self.pool):
            self.log.info("Task is not able to be run")
            return

        try:
            self.task_runner.start()

            heartbeat_time_limit = conf.getint('scheduler',
                                               'scheduler_zombie_task_threshold')
            while True:
                # Monitor the task to see if it's done
                return_code = self.task_runner.return_code()
                if return_code is not None:
                    self.log.info("Task exited with return code %s", return_code)
                    return

                self.heartbeat()

                # If it's been too long since we've heartbeat, then it's possible that
                # the scheduler rescheduled this task, so kill launched processes.
                # This can only really happen if the worker can't read the DB for a long time
                time_since_last_heartbeat = (timezone.utcnow() - self.latest_heartbeat).total_seconds()
                if time_since_last_heartbeat > heartbeat_time_limit:
                    Stats.incr('local_task_job_prolonged_heartbeat_failure', 1, 1)
                    self.log.error("Heartbeat time limit exceeded!")
                    raise AirflowException("Time since last heartbeat({:.2f}s) "
                                           "exceeded limit ({}s)."
                                           .format(time_since_last_heartbeat,
                                                   heartbeat_time_limit))
        finally:
            self.on_kill()

    def on_kill(self):
        self.task_runner.terminate()
        self.task_runner.on_finish()

    @provide_session
    def heartbeat_callback(self, session=None):
        """Self destruct task if state has been moved away from running externally"""

        if self.terminating:
            # ensure termination if processes are created later
            self.task_runner.terminate()
            return

        self.task_instance.refresh_from_db()
        ti = self.task_instance

        if ti.state == State.RUNNING:
            fqdn = get_hostname()
            same_hostname = fqdn == ti.hostname
            if not same_hostname:
                self.log.warning("The recorded hostname %s "
                                 "does not match this instance's hostname "
                                 "%s", ti.hostname, fqdn)
                raise AirflowException("Hostname of job runner does not match")

            current_pid = os.getpid()
            same_process = ti.pid == current_pid
            if not same_process:
                self.log.warning("Recorded pid %s does not match "
                                 "the current pid %s", ti.pid, current_pid)
                raise AirflowException("PID of job runner does not match")
        elif (
                self.task_runner.return_code() is None and
                hasattr(self.task_runner, 'process')
        ):
            self.log.warning(
                "State of this instance has been externally set to %s. "
                "Taking the poison pill.",
                ti.state
            )
            if ti.state == State.FAILED and ti.task.on_failure_callback:
                context = ti.get_template_context()
                ti.task.on_failure_callback(context)
            if ti.state == State.SUCCESS and ti.task.on_success_callback:
                context = ti.get_template_context()
                ti.task.on_success_callback(context)
            self.task_runner.terminate()
            self.terminating = True
```

## Scheduler Job

此调度程序将运行特定的时间间隔，并调度准备运行的作业。它计算出每个任务的最新运行情况，并查看是否满足了下一个调度的依赖项。如果是，它将创建适当的taskinstance并向执行程序发送run命令。它对每个DAG中的每个任务都这样做并重复。

```python
import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from contextlib import redirect_stderr, redirect_stdout, suppress
from datetime import timedelta
from itertools import groupby
from typing import Dict, List, Optional, Tuple

from setproctitle import setproctitle
from sqlalchemy import and_, func, not_, or_
from sqlalchemy.orm.session import make_transient

from airflow import models, settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException, TaskNotFound
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagModel, SlaMiss, errors
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKeyType
from airflow.operators.dummy_operator import DummyOperator
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULED_DEPS
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.utils import asciiart, helpers, timezone
from airflow.utils.dag_processing import (
    AbstractDagFileProcessorProcess, DagFileProcessorAgent, FailureCallbackRequest, SimpleDag, SimpleDagBag,
)
from airflow.utils.email import get_email_address_list, send_email
from airflow.utils.log.logging_mixin import LoggingMixin, StreamLogWriter, set_context
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType


class DagFileProcessorProcess(AbstractDagFileProcessorProcess, LoggingMixin):
    """Runs DAG processing in a separate process using DagFileProcessor

    :param file_path: a Python file containing Airflow DAG definitions
    :type file_path: str
    :param pickle_dags: whether to serialize the DAG objects to the DB
    :type pickle_dags: bool
    :param dag_id_white_list: If specified, only look at these DAG ID's
    :type dag_id_white_list: List[str]
    :param failure_callback_requests: failure callback to execute
    :type failure_callback_requests: List[airflow.utils.dag_processing.FailureCallbackRequest]
    """

    # Counter that increments every time an instance of this class is created
    class_creation_counter = 0

    def __init__(
        self,
        file_path: str,
        pickle_dags: bool,
        dag_id_white_list: Optional[List[str]],
        failure_callback_requests: List[FailureCallbackRequest]
    ):
        super().__init__()
        self._file_path = file_path
        self._pickle_dags = pickle_dags
        self._dag_id_white_list = dag_id_white_list
        self._failure_callback_requests = failure_callback_requests

        # The process that was launched to process the given .
        self._process = None
        # The result of Scheduler.process_file(file_path).
        self._result = None
        # Whether the process is done running.
        self._done = False
        # When the process started.
        self._start_time = None
        # This ID is use to uniquely name the process / thread that's launched
        # by this processor instance
        self._instance_id = DagFileProcessorProcess.class_creation_counter

        self._parent_channel = None
        self._result_queue = None
        DagFileProcessorProcess.class_creation_counter += 1

    @property
    def file_path(self):
        return self._file_path

    @staticmethod
    def _run_file_processor(result_channel,
                            file_path,
                            pickle_dags,
                            dag_id_white_list,
                            thread_name,
                            failure_callback_requests):
        """
        Process the given file.

        :param result_channel: the connection to use for passing back the result
        :type result_channel: multiprocessing.Connection
        :param file_path: the file to process
        :type file_path: str
        :param pickle_dags: whether to pickle the DAGs found in the file and
            save them to the DB
        :type pickle_dags: bool
        :param dag_id_white_list: if specified, only examine DAG ID's that are
            in this list
        :type dag_id_white_list: list[str]
        :param thread_name: the name to use for the process that is launched
        :type thread_name: str
        :param failure_callback_requests: failure callback to execute
        :type failure_callback_requests: list[airflow.utils.dag_processing.FailureCallbackRequest]
        :return: the process that was launched
        :rtype: multiprocessing.Process
        """
        # This helper runs in the newly created process
        log = logging.getLogger("airflow.processor")

        set_context(log, file_path)
        setproctitle("airflow scheduler - DagFileProcessor {}".format(file_path))

        try:
            # redirect stdout/stderr to log
            with redirect_stdout(StreamLogWriter(log, logging.INFO)),\
                    redirect_stderr(StreamLogWriter(log, logging.WARN)):

                # Re-configure the ORM engine as there are issues with multiple processes
                settings.configure_orm()

                # Change the thread name to differentiate log lines. This is
                # really a separate process, but changing the name of the
                # process doesn't work, so changing the thread name instead.
                threading.current_thread().name = thread_name
                start_time = time.time()

                log.info("Started process (PID=%s) to work on %s", os.getpid(), file_path)
                dag_file_processor = DagFileProcessor(dag_ids=dag_id_white_list, log=log)
                result = dag_file_processor.process_file(
                    file_path=file_path,
                    pickle_dags=pickle_dags,
                    failure_callback_requests=failure_callback_requests,
                )
                result_channel.send(result)
                end_time = time.time()
                log.info(
                    "Processing %s took %.3f seconds", file_path, end_time - start_time
                )
        except Exception:  # pylint: disable=broad-except
            # Log exceptions through the logging framework.
            log.exception("Got an exception! Propagating...")
            raise
        finally:
            result_channel.close()
            # We re-initialized the ORM within this Process above so we need to
            # tear it down manually here
            settings.dispose_orm()

    def start(self):
        """
        Launch the process and start processing the DAG.
        """
        self._parent_channel, _child_channel = multiprocessing.Pipe()
        self._process = multiprocessing.Process(
            target=type(self)._run_file_processor,
            args=(
                _child_channel,
                self.file_path,
                self._pickle_dags,
                self._dag_id_white_list,
                "DagFileProcessor{}".format(self._instance_id),
                self._failure_callback_requests
            ),
            name="DagFileProcessor{}-Process".format(self._instance_id)
        )
        self._start_time = timezone.utcnow()
        self._process.start()

    def kill(self):
        """
        Kill the process launched to process the file, and ensure consistent state.
        """
        if self._process is None:
            raise AirflowException("Tried to kill before starting!")
        # The queue will likely get corrupted, so remove the reference
        self._result_queue = None
        self._kill_process()

    def terminate(self, sigkill=False):
        """
        Terminate (and then kill) the process launched to process the file.

        :param sigkill: whether to issue a SIGKILL if SIGTERM doesn't work.
        :type sigkill: bool
        """
        if self._process is None:
            raise AirflowException("Tried to call terminate before starting!")

        self._process.terminate()
        # Arbitrarily wait 5s for the process to die
        with suppress(TimeoutError):
            self._process._popen.wait(5)  # pylint: disable=protected-access
        if sigkill:
            self._kill_process()
        self._parent_channel.close()

    def _kill_process(self):
        if self._process.is_alive():
            self.log.warning("Killing PID %s", self._process.pid)
            os.kill(self._process.pid, signal.SIGKILL)

    @property
    def pid(self):
        """
        :return: the PID of the process launched to process the given file
        :rtype: int
        """
        if self._process is None:
            raise AirflowException("Tried to get PID before starting!")
        return self._process.pid

    @property
    def exit_code(self):
        """
        After the process is finished, this can be called to get the return code

        :return: the exit code of the process
        :rtype: int
        """
        if not self._done:
            raise AirflowException("Tried to call retcode before process was finished!")
        return self._process.exitcode

    @property
    def done(self):
        """
        Check if the process launched to process this file is done.

        :return: whether the process is finished running
        :rtype: bool
        """
        if self._process is None:
            raise AirflowException("Tried to see if it's done before starting!")

        if self._done:
            return True

        if self._parent_channel.poll():
            try:
                self._result = self._parent_channel.recv()
                self._done = True
                self.log.debug("Waiting for %s", self._process)
                self._process.join()
                self._parent_channel.close()
                return True
            except EOFError:
                pass

        if not self._process.is_alive():
            self._done = True
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            self._parent_channel.close()
            return True

        return False

    @property
    def result(self):
        """
        :return: result of running SchedulerJob.process_file()
        :rtype: airflow.utils.dag_processing.SimpleDag
        """
        if not self.done:
            raise AirflowException("Tried to get the result before it's done!")
        return self._result

    @property
    def start_time(self):
        """
        :return: when this started to process the file
        :rtype: datetime
        """
        if self._start_time is None:
            raise AirflowException("Tried to get start time before it started!")
        return self._start_time


class DagFileProcessor(LoggingMixin):
    """
    Process a Python file containing Airflow DAGs.

    This includes:

    1. Execute the file and look for DAG objects in the namespace.
    2. Pickle the DAG and save it to the DB (if necessary).
    3. For each DAG, see what tasks should run and create appropriate task
    instances in the DB.
    4. Record any errors importing the file into ORM
    5. Kill (in ORM) any task instances belonging to the DAGs that haven't
    issued a heartbeat in a while.

    Returns a list of SimpleDag objects that represent the DAGs found in
    the file

    :param dag_ids: If specified, only look at these DAG ID's
    :type dag_ids: List[str]
    :param log: Logger to save the processing process
    :type log: logging.Logger
    """

    UNIT_TEST_MODE = conf.getboolean('core', 'UNIT_TEST_MODE')

    def __init__(self, dag_ids, log):
        super().__init__()
        self.dag_ids = dag_ids
        self._log = log

    @provide_session
    def manage_slas(self, dag, session=None):
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        We are assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        if not any([isinstance(ti.sla, timedelta) for ti in dag.tasks]):
            self.log.info("Skipping SLA check for %s because no tasks in DAG have SLAs", dag)
            return

        TI = models.TaskInstance
        qry = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti')
            )
            .with_hint(TI, 'USE INDEX (PRIMARY)', dialect_name='mysql')
            .filter(TI.dag_id == dag.dag_id)
            .filter(
                or_(
                    TI.state == State.SUCCESS,
                    TI.state == State.SKIPPED
                )
            )
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id).subquery('sq')
        )

        max_tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == qry.c.task_id,
            TI.execution_date == qry.c.max_ti,
        ).all()

        ts = timezone.utcnow()
        for ti in max_tis:  # pylint: disable=too-many-nested-blocks
            task = dag.get_task(ti.task_id)
            dttm = ti.execution_date
            if isinstance(task.sla, timedelta):
                dttm = dag.following_schedule(dttm)
                while dttm < timezone.utcnow():
                    following_schedule = dag.following_schedule(dttm)
                    if following_schedule + task.sla < timezone.utcnow():
                        session.merge(SlaMiss(
                            task_id=ti.task_id,
                            dag_id=ti.dag_id,
                            execution_date=dttm,
                            timestamp=ts))
                    dttm = dag.following_schedule(dttm)
        session.commit()

        slas = (
            session
            .query(SlaMiss)
            .filter(SlaMiss.notification_sent == False, SlaMiss.dag_id == dag.dag_id)  # noqa pylint: disable=singleton-comparison
            .all()
        )

        if slas:  # pylint: disable=too-many-nested-blocks
            sla_dates = [sla.execution_date for sla in slas]
            qry = (
                session
                .query(TI)
                .filter(
                    TI.state != State.SUCCESS,
                    TI.execution_date.in_(sla_dates),
                    TI.dag_id == dag.dag_id
                ).all()
            )
            blocking_tis = []
            for ti in qry:
                if ti.task_id in dag.task_ids:
                    ti.task = dag.get_task(ti.task_id)
                    blocking_tis.append(ti)
                else:
                    session.delete(ti)
                    session.commit()

            task_list = "\n".join([
                sla.task_id + ' on ' + sla.execution_date.isoformat()
                for sla in slas])
            blocking_task_list = "\n".join([
                ti.task_id + ' on ' + ti.execution_date.isoformat()
                for ti in blocking_tis])
            # Track whether email or any alert notification sent
            # We consider email or the alert callback as notifications
            email_sent = False
            notification_sent = False
            if dag.sla_miss_callback:
                # Execute the alert callback
                self.log.info(' --------------> ABOUT TO CALL SLA MISS CALL BACK ')
                try:
                    dag.sla_miss_callback(dag, task_list, blocking_task_list, slas,
                                          blocking_tis)
                    notification_sent = True
                except Exception:  # pylint: disable=broad-except
                    self.log.exception("Could not call sla_miss_callback for DAG %s",
                                       dag.dag_id)
            email_content = """\
            Here's a list of tasks that missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{bug}<code></pre>
            """.format(task_list=task_list, blocking_task_list=blocking_task_list,
                       bug=asciiart.bug)

            tasks_missed_sla = []
            for sla in slas:
                try:
                    task = dag.get_task(sla.task_id)
                except TaskNotFound:
                    # task already deleted from DAG, skip it
                    self.log.warning(
                        "Task %s doesn't exist in DAG anymore, skipping SLA miss notification.",
                        sla.task_id)
                    continue
                tasks_missed_sla.append(task)

            emails = set()
            for task in tasks_missed_sla:
                if task.email:
                    if isinstance(task.email, str):
                        emails |= set(get_email_address_list(task.email))
                    elif isinstance(task.email, (list, tuple)):
                        emails |= set(task.email)
            if emails:
                try:
                    send_email(
                        emails,
                        "[airflow] SLA miss on DAG=" + dag.dag_id,
                        email_content)
                    email_sent = True
                    notification_sent = True
                except Exception:  # pylint: disable=broad-except
                    self.log.exception("Could not send SLA Miss email notification for"
                                       " DAG %s", dag.dag_id)
            # If we sent any notification, update the sla_miss table
            if notification_sent:
                for sla in slas:
                    if email_sent:
                        sla.email_sent = True
                    sla.notification_sent = True
                    session.merge(sla)
            session.commit()

    @staticmethod
    def update_import_errors(session, dagbag):
        """
        For the DAGs in the given DagBag, record any associated import errors and clears
        errors for files that no longer have them. These are usually displayed through the
        Airflow UI so that users know that there are issues parsing DAGs.

        :param session: session for ORM operations
        :type session: sqlalchemy.orm.session.Session
        :param dagbag: DagBag containing DAGs with import errors
        :type dagbag: airflow.models.DagBag
        """
        # Clear the errors of the processed files
        for dagbag_file in dagbag.file_last_changed:
            session.query(errors.ImportError).filter(
                errors.ImportError.filename == dagbag_file
            ).delete()

        # Add the errors of the processed files
        for filename, stacktrace in dagbag.import_errors.items():
            session.add(errors.ImportError(
                filename=filename,
                timestamp=timezone.utcnow(),
                stacktrace=stacktrace))
        session.commit()

    # pylint: disable=too-many-return-statements,too-many-branches
    @provide_session
    def create_dag_run(self, dag, dag_runs=None, session=None):
        """
        This method checks whether a new DagRun needs to be created
        for a DAG based on scheduling interval.
        Returns DagRun if one is scheduled. Otherwise returns None.
        """
        # pylint: disable=too-many-nested-blocks
        if not dag.schedule_interval:
            return None

        if dag_runs is None:
            active_runs = DagRun.find(
                dag_id=dag.dag_id,
                state=State.RUNNING,
                external_trigger=False,
                session=session
            )
        else:
            active_runs = [
                dag_run
                for dag_run in dag_runs
                if not dag_run.external_trigger
            ]
        # return if already reached maximum active runs and no timeout setting
        if len(active_runs) >= dag.max_active_runs and not dag.dagrun_timeout:
            return None
        timedout_runs = 0
        for dr in active_runs:
            if (
                dr.start_date and dag.dagrun_timeout and
                dr.start_date < timezone.utcnow() - dag.dagrun_timeout
            ):
                dr.state = State.FAILED
                dr.end_date = timezone.utcnow()
                dag.handle_callback(dr, success=False, reason='dagrun_timeout',
                                    session=session)
                timedout_runs += 1
        session.commit()
        if len(active_runs) - timedout_runs >= dag.max_active_runs:
            return None

        # this query should be replaced by find dagrun
        qry = (
            session.query(func.max(DagRun.execution_date))
                .filter_by(dag_id=dag.dag_id)
                .filter(or_(
                    DagRun.external_trigger == False,  # noqa: E712 pylint: disable=singleton-comparison
                    # add % as a wildcard for the like query
                    DagRun.run_id.like(f"{DagRunType.SCHEDULED.value}__%")
                )
            )
        )
        last_scheduled_run = qry.scalar()

        # don't schedule @once again
        if dag.schedule_interval == '@once' and last_scheduled_run:
            return None

        # don't do scheduler catchup for dag's that don't have dag.catchup = True
        if not (dag.catchup or dag.schedule_interval == '@once'):
            # The logic is that we move start_date up until
            # one period before, so that timezone.utcnow() is AFTER
            # the period end, and the job can be created...
            now = timezone.utcnow()
            next_start = dag.following_schedule(now)
            last_start = dag.previous_schedule(now)
            if next_start <= now:
                new_start = last_start
            else:
                new_start = dag.previous_schedule(last_start)

            if dag.start_date:
                if new_start >= dag.start_date:
                    dag.start_date = new_start
            else:
                dag.start_date = new_start

        next_run_date = None
        if not last_scheduled_run:
            # First run
            task_start_dates = [t.start_date for t in dag.tasks]
            if task_start_dates:
                next_run_date = dag.normalize_schedule(min(task_start_dates))
                self.log.debug(
                    "Next run date based on tasks %s",
                    next_run_date
                )
        else:
            next_run_date = dag.following_schedule(last_scheduled_run)

        # make sure backfills are also considered
        last_run = dag.get_last_dagrun(session=session)
        if last_run and next_run_date:
            while next_run_date <= last_run.execution_date:
                next_run_date = dag.following_schedule(next_run_date)

        # don't ever schedule prior to the dag's start_date
        if dag.start_date:
            next_run_date = (dag.start_date if not next_run_date
                             else max(next_run_date, dag.start_date))
            if next_run_date == dag.start_date:
                next_run_date = dag.normalize_schedule(dag.start_date)

            self.log.debug(
                "Dag start date: %s. Next run date: %s",
                dag.start_date, next_run_date
            )

        # don't ever schedule in the future or if next_run_date is None
        if not next_run_date or next_run_date > timezone.utcnow():
            return None

        # this structure is necessary to avoid a TypeError from concatenating
        # NoneType
        if dag.schedule_interval == '@once':
            period_end = next_run_date
        elif next_run_date:
            period_end = dag.following_schedule(next_run_date)

        # Don't schedule a dag beyond its end_date (as specified by the dag param)
        if next_run_date and dag.end_date and next_run_date > dag.end_date:
            return None

        # Don't schedule a dag beyond its end_date (as specified by the task params)
        # Get the min task end date, which may come from the dag.default_args
        min_task_end_date = []
        task_end_dates = [t.end_date for t in dag.tasks if t.end_date]
        if task_end_dates:
            min_task_end_date = min(task_end_dates)
        if next_run_date and min_task_end_date and next_run_date > min_task_end_date:
            return None

        if next_run_date and period_end and period_end <= timezone.utcnow():
            next_run = dag.create_dagrun(
                run_id=f"{DagRunType.SCHEDULED.value}__{next_run_date.isoformat()}",
                execution_date=next_run_date,
                start_date=timezone.utcnow(),
                state=State.RUNNING,
                external_trigger=False
            )
            return next_run

        return None

    @provide_session
    def _process_task_instances(
        self, dag: DAG, dag_runs: List[DagRun], session=None
    ) -> List[TaskInstanceKeyType]:
        """
        This method schedules the tasks for a single DAG by looking at the
        active DAG runs and adding task instances that should run to the
        queue.
        """

        # update the state of the previously active dag runs
        active_dag_runs = 0
        task_instances_list = []
        for run in dag_runs:
            self.log.info("Examining DAG run %s", run)
            # don't consider runs that are executed in the future unless
            # specified by config and schedule_interval is None
            if run.execution_date > timezone.utcnow() and not dag.allow_future_exec_dates:
                self.log.error(
                    "Execution date is in future: %s",
                    run.execution_date
                )
                continue

            if active_dag_runs >= dag.max_active_runs:
                self.log.info("Number of active dag runs reached max_active_run.")
                break

            # skip backfill dagruns for now as long as they are not really scheduled
            if run.is_backfill:
                continue

            # todo: run.dag is transient but needs to be set
            run.dag = dag  # type: ignore
            # todo: preferably the integrity check happens at dag collection time
            run.verify_integrity(session=session)
            ready_tis = run.update_state(session=session)
            if run.state == State.RUNNING:
                active_dag_runs += 1
                self.log.debug("Examining active DAG run: %s", run)
                for ti in ready_tis:
                    self.log.debug('Queuing task: %s', ti)
                    task_instances_list.append(ti.key)
        return task_instances_list

    @provide_session
    def _process_dags(self, dags: List[DAG], session=None):
        """
        Iterates over the dags and processes them. Processing includes:

        1. Create appropriate DagRun(s) in the DB.
        2. Create appropriate TaskInstance(s) in the DB.
        3. Send emails for tasks that have missed SLAs (if CHECK_SLAS config enabled).

        :param dags: the DAGs from the DagBag to process
        :type dags: List[airflow.models.DAG]
        :rtype: list[TaskInstance]
        :return: A list of generated TaskInstance objects
        """
        check_slas = conf.getboolean('core', 'CHECK_SLAS', fallback=True)
        use_job_schedule = conf.getboolean('scheduler', 'USE_JOB_SCHEDULE')

        # pylint: disable=too-many-nested-blocks
        tis_out: List[TaskInstanceKeyType] = []
        dag_ids = [dag.dag_id for dag in dags]
        dag_runs = DagRun.find(dag_id=dag_ids, state=State.RUNNING, session=session)
        # As per the docs of groupby (https://docs.python.org/3/library/itertools.html#itertools.groupby)
        # we need to use `list()` otherwise the result will be wrong/incomplete
        dag_runs_by_dag_id = {k: list(v) for k, v in groupby(dag_runs, lambda d: d.dag_id)}

        for dag in dags:
            dag_id = dag.dag_id
            self.log.info("Processing %s", dag_id)
            dag_runs_for_dag = dag_runs_by_dag_id.get(dag_id) or []

            # Only creates DagRun for DAGs that are not subdag since
            # DagRun of subdags are created when SubDagOperator executes.
            if not dag.is_subdag and use_job_schedule:
                dag_run = self.create_dag_run(dag, dag_runs=dag_runs_for_dag)
                if dag_run:
                    dag_runs_for_dag.append(dag_run)
                    expected_start_date = dag.following_schedule(dag_run.execution_date)
                    if expected_start_date:
                        schedule_delay = dag_run.start_date - expected_start_date
                        Stats.timing(
                            'dagrun.schedule_delay.{dag_id}'.format(dag_id=dag.dag_id),
                            schedule_delay)
                    self.log.info("Created %s", dag_run)

            if dag_runs_for_dag:
                tis_out.extend(self._process_task_instances(dag, dag_runs_for_dag))
                if check_slas:
                    self.manage_slas(dag)

        return tis_out

    def _find_dags_to_process(self, dags: List[DAG]) -> List[DAG]:
        """
        Find the DAGs that are not paused to process.

        :param dags: specified DAGs
        :return: DAGs to process
        """
        if self.dag_ids:
            dags = [dag for dag in dags
                    if dag.dag_id in self.dag_ids]
        return dags

    @provide_session
    def execute_on_failure_callbacks(self, dagbag, failure_callback_requests, session=None):
        """
        Execute on failure callbacks. These objects can come from SchedulerJob or from
        DagFileProcessorManager.

        :param failure_callback_requests: failure callbacks to execute
        :type failure_callback_requests: List[airflow.utils.dag_processing.FailureCallbackRequest]
        :param session: DB session.
        """
        TI = models.TaskInstance

        for request in failure_callback_requests:
            simple_ti = request.simple_task_instance
            if simple_ti.dag_id in dagbag.dags:
                dag = dagbag.dags[simple_ti.dag_id]
                if simple_ti.task_id in dag.task_ids:
                    task = dag.get_task(simple_ti.task_id)
                    ti = TI(task, simple_ti.execution_date)
                    # Get properties needed for failure handling from SimpleTaskInstance.
                    ti.start_date = simple_ti.start_date
                    ti.end_date = simple_ti.end_date
                    ti.try_number = simple_ti.try_number
                    ti.state = simple_ti.state
                    ti.test_mode = self.UNIT_TEST_MODE
                    ti.handle_failure(request.msg, ti.test_mode, ti.get_template_context())
                    self.log.info('Executed failure callback for %s in state %s', ti, ti.state)
        session.commit()

    @provide_session
    def process_file(
        self, file_path, failure_callback_requests, pickle_dags=False, session=None
    ) -> Tuple[List[SimpleDag], int]:
        """
        Process a Python file containing Airflow DAGs.

        This includes:

        1. Execute the file and look for DAG objects in the namespace.
        2. Pickle the DAG and save it to the DB (if necessary).
        3. For each DAG, see what tasks should run and create appropriate task
        instances in the DB.
        4. Record any errors importing the file into ORM
        5. Kill (in ORM) any task instances belonging to the DAGs that haven't
        issued a heartbeat in a while.

        Returns a list of SimpleDag objects that represent the DAGs found in
        the file

        :param file_path: the path to the Python file that should be executed
        :type file_path: str
        :param failure_callback_requests: failure callback to execute
        :type failure_callback_requests: List[airflow.utils.dag_processing.FailureCallbackRequest]
        :param pickle_dags: whether serialize the DAGs found in the file and
            save them to the db
        :type pickle_dags: bool
        :return: a tuple with list of SimpleDags made from the Dags found in the file and
            count of import errors.
        :rtype: Tuple[List[SimpleDag], int]
        """
        self.log.info("Processing file %s for tasks to queue", file_path)

        try:
            dagbag = models.DagBag(file_path, include_examples=False)
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Failed at reloading the DAG file %s", file_path)
            Stats.incr('dag_file_refresh_error', 1, 1)
            return [], 0

        if len(dagbag.dags) > 0:
            self.log.info("DAG(s) %s retrieved from %s", dagbag.dags.keys(), file_path)
        else:
            self.log.warning("No viable dags retrieved from %s", file_path)
            self.update_import_errors(session, dagbag)
            return [], len(dagbag.import_errors)

        try:
            self.execute_on_failure_callbacks(dagbag, failure_callback_requests)
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Error executing failure callback!")

        # Save individual DAGs in the ORM and update DagModel.last_scheduled_time
        dagbag.sync_to_db()

        paused_dag_ids = DagModel.get_paused_dag_ids(dag_ids=dagbag.dag_ids)

        unpaused_dags = [dag for dag_id, dag in dagbag.dags.items() if dag_id not in paused_dag_ids]

        simple_dags = self._prepare_simple_dags(unpaused_dags, pickle_dags, session)

        dags = self._find_dags_to_process(unpaused_dags)

        ti_keys_to_schedule = self._process_dags(dags, session)

        self._schedule_task_instances(dagbag, ti_keys_to_schedule, session)

        # Record import errors into the ORM
        try:
            self.update_import_errors(session, dagbag)
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Error logging import errors!")

        return simple_dags, len(dagbag.import_errors)

    @provide_session
    def _schedule_task_instances(
        self,
        dagbag: models.DagBag,
        ti_keys_to_schedule: List[TaskInstanceKeyType],
        session=None
    ) -> None:
        """
        Checks whether the tasks specified by `ti_keys_to_schedule` parameter can be scheduled and
        updates the information in the database,

        :param dagbag: DagBag
        :type dagbag: models.DagBag
        :param ti_keys_to_schedule:  List of task instnace keys which can be scheduled.
        :param ti_keys_to_schedule:
        """
        # Refresh all task instances that will be scheduled
        TI = models.TaskInstance
        filter_for_tis = TI.filter_for_tis(ti_keys_to_schedule)

        refreshed_tis: List[models.TaskInstance] = []

        if filter_for_tis is not None:
            refreshed_tis = session.query(TI).filter(filter_for_tis).with_for_update().all()

        for ti in refreshed_tis:
            # Add task to task instance
            dag = dagbag.dags[ti.key[0]]
            ti.task = dag.get_task(ti.key[1])

            # We check only deps needed to set TI to SCHEDULED state here.
            # Deps needed to set TI to QUEUED state will be batch checked later
            # by the scheduler for better performance.
            dep_context = DepContext(deps=SCHEDULED_DEPS, ignore_task_deps=True)

            # Only schedule tasks that have their dependencies met, e.g. to avoid
            # a task that recently got its state changed to RUNNING from somewhere
            # other than the scheduler from getting its state overwritten.
            if ti.are_dependencies_met(
                dep_context=dep_context,
                session=session,
                verbose=True
            ):
                # Task starts out in the scheduled state. All tasks in the
                # scheduled state will be sent to the executor
                ti.state = State.SCHEDULED
                # If the task is dummy, then mark it as done automatically
                if isinstance(ti.task, DummyOperator) \
                        and not ti.task.on_execute_callback \
                        and not ti.task.on_success_callback:
                    ti.state = State.SUCCESS

            # Also save this task instance to the DB.
            self.log.info("Creating / updating %s in ORM", ti)
            session.merge(ti)
        # commit batch
        session.commit()

    @provide_session
    def _prepare_simple_dags(self, dags: List[DAG], pickle_dags: bool, session=None) -> List[SimpleDag]:
        """
        Convert DAGS to  SimpleDags. If necessary, it also Pickle the DAGs

        :param dags: List of DAGs
        :param pickle_dags: whether serialize the DAGs found in the file and
            save them to the db
        :type pickle_dags: bool
        :return: List of SimpleDag
        :rtype: List[airflow.utils.dag_processing.SimpleDag]
        """

        simple_dags = []
        # Pickle the DAGs (if necessary) and put them into a SimpleDag
        for dag in dags:
            pickle_id = dag.pickle(session).id if pickle_dags else None
            simple_dags.append(SimpleDag(dag, pickle_id=pickle_id))
        return simple_dags


class SchedulerJob(BaseJob):
    """
    This SchedulerJob runs for a specific time interval and schedules the jobs
    that are ready to run. It figures out the latest runs for each
    task and sees if the dependencies for the next schedules are met.
    If so, it creates appropriate TaskInstances and sends run commands to the
    executor. It does this for each task in each DAG and repeats.

    :param dag_id: if specified, only schedule tasks with this DAG ID
    :type dag_id: str
    :param dag_ids: if specified, only schedule tasks with these DAG IDs
    :type dag_ids: list[str]
    :param subdir: directory containing Python files with Airflow DAG
        definitions, or a specific path to a file
    :type subdir: str
    :param num_runs: The number of times to try to schedule each DAG file.
        -1 for unlimited times.
    :type num_runs: int
    :param processor_poll_interval: The number of seconds to wait between
        polls of running processors
    :type processor_poll_interval: int
    :param do_pickle: once a DAG object is obtained by executing the Python
        file, whether to serialize the DAG object to the DB
    :type do_pickle: bool
    """

    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerJob'
    }
    heartrate = conf.getint('scheduler', 'SCHEDULER_HEARTBEAT_SEC')

    def __init__(
            self,
            dag_id=None,
            dag_ids=None,
            subdir=settings.DAGS_FOLDER,
            num_runs=conf.getint('scheduler', 'num_runs'),
            processor_poll_interval=conf.getfloat('scheduler', 'processor_poll_interval'),
            do_pickle=False,
            log=None,
            *args, **kwargs):
        # for BaseJob compatibility
        self.dag_id = dag_id
        self.dag_ids = [dag_id] if dag_id else []
        if dag_ids:
            self.dag_ids.extend(dag_ids)

        self.subdir = subdir

        self.num_runs = num_runs
        self._processor_poll_interval = processor_poll_interval

        self.do_pickle = do_pickle
        super().__init__(*args, **kwargs)

        self.max_threads = conf.getint('scheduler', 'max_threads')

        if log:
            self._log = log

        self.using_sqlite = False
        self.using_mysql = False
        if conf.get('core', 'sql_alchemy_conn').lower().startswith('sqlite'):
            self.using_sqlite = True
        if conf.get('core', 'sql_alchemy_conn').lower().startswith('mysql'):
            self.using_mysql = True

        self.max_tis_per_query = conf.getint('scheduler', 'max_tis_per_query')
        self.processor_agent = None

    def register_exit_signals(self):
        """
        Register signals that stop child processes
        """
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):  # pylint: disable=unused-argument
        """
        Helper method to clean up processor_agent to avoid leaving orphan processes.
        """
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        if self.processor_agent:
            self.processor_agent.end()
        sys.exit(os.EX_OK)

    def is_alive(self, grace_multiplier=None):
        """
        Is this SchedulerJob alive?

        We define alive as in a state of running and a heartbeat within the
        threshold defined in the ``scheduler_health_check_threshold`` config
        setting.

        ``grace_multiplier`` is accepted for compatibility with the parent class.

        :rtype: boolean
        """
        if grace_multiplier is not None:
            # Accept the same behaviour as superclass
            return super().is_alive(grace_multiplier=grace_multiplier)
        scheduler_health_check_threshold = conf.getint('scheduler', 'scheduler_health_check_threshold')
        return (
            self.state == State.RUNNING and
            (timezone.utcnow() - self.latest_heartbeat).total_seconds() < scheduler_health_check_threshold
        )

    @provide_session
    def _change_state_for_tis_without_dagrun(self,
                                             simple_dag_bag,
                                             old_states,
                                             new_state,
                                             session=None):
        """
        For all DAG IDs in the SimpleDagBag, look for task instances in the
        old_states and set them to new_state if the corresponding DagRun
        does not exist or exists but is not in the running state. This
        normally should not happen, but it can if the state of DagRuns are
        changed manually.

        :param old_states: examine TaskInstances in this state
        :type old_states: list[airflow.utils.state.State]
        :param new_state: set TaskInstances to this state
        :type new_state: airflow.utils.state.State
        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag and with states in the old_states will be examined
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        """
        tis_changed = 0
        query = session \
            .query(models.TaskInstance) \
            .outerjoin(models.DagRun, and_(
                models.TaskInstance.dag_id == models.DagRun.dag_id,
                models.TaskInstance.execution_date == models.DagRun.execution_date)) \
            .filter(models.TaskInstance.dag_id.in_(simple_dag_bag.dag_ids)) \
            .filter(models.TaskInstance.state.in_(old_states)) \
            .filter(or_(
                models.DagRun.state != State.RUNNING,
                models.DagRun.state.is_(None)))  # pylint: disable=no-member
        # We need to do this for mysql as well because it can cause deadlocks
        # as discussed in https://issues.apache.org/jira/browse/AIRFLOW-2516
        if self.using_sqlite or self.using_mysql:
            tis_to_change = query \
                .with_for_update() \
                .all()
            for ti in tis_to_change:
                ti.set_state(new_state, session=session)
                tis_changed += 1
        else:
            subq = query.subquery()
            tis_changed = session \
                .query(models.TaskInstance) \
                .filter(and_(
                    models.TaskInstance.dag_id == subq.c.dag_id,
                    models.TaskInstance.task_id == subq.c.task_id,
                    models.TaskInstance.execution_date ==
                    subq.c.execution_date)) \
                .update({models.TaskInstance.state: new_state}, synchronize_session=False)
            session.commit()

        if tis_changed > 0:
            self.log.warning(
                "Set %s task instances to state=%s as their associated DagRun was not in RUNNING state",
                tis_changed, new_state
            )
            Stats.gauge('scheduler.tasks.without_dagrun', tis_changed)

    @provide_session
    def __get_concurrency_maps(self, states: frozenset, session=None):
        """
        Get the concurrency maps.

        :param states: List of states to query for
        :type states: list[airflow.utils.state.State]
        :return: A map from (dag_id, task_id) to # of task instances and
         a map from (dag_id, task_id) to # of task instances in the given state list
        :rtype: dict[tuple[str, str], int]

        """
        TI = models.TaskInstance
        ti_concurrency_query = (
            session
            .query(TI.task_id, TI.dag_id, func.count('*'))
            .filter(TI.state.in_(states))
            .group_by(TI.task_id, TI.dag_id)
        ).all()
        dag_map: Dict[str, int] = defaultdict(int)
        task_map: Dict[Tuple[str, str], int] = defaultdict(int)
        for result in ti_concurrency_query:
            task_id, dag_id, count = result
            dag_map[dag_id] += count
            task_map[(dag_id, task_id)] = count
        return dag_map, task_map

    # pylint: disable=too-many-locals,too-many-statements
    @provide_session
    def _find_executable_task_instances(self, simple_dag_bag, session=None):
        """
        Finds TIs that are ready for execution with respect to pool limits,
        dag concurrency, executor state, and priority.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        :return: list[airflow.models.TaskInstance]
        """
        executable_tis = []

        # Get all task instances associated with scheduled
        # DagRuns which are not backfilled, in the given states,
        # and the dag is not paused
        TI = models.TaskInstance
        DR = models.DagRun
        DM = models.DagModel
        task_instances_to_examine = (
            session
            .query(TI)
            .filter(TI.dag_id.in_(simple_dag_bag.dag_ids))
            .outerjoin(
                DR, and_(DR.dag_id == TI.dag_id, DR.execution_date == TI.execution_date)
            )
            .filter(or_(DR.run_id.is_(None), not_(DR.run_id.like(f"{DagRunType.BACKFILL_JOB.value}__%"))))
            .outerjoin(DM, DM.dag_id == TI.dag_id)
            .filter(or_(DM.dag_id.is_(None), not_(DM.is_paused)))
            .filter(TI.state == State.SCHEDULED)
            .all()
        )
        Stats.gauge('scheduler.tasks.pending', len(task_instances_to_examine))

        if len(task_instances_to_examine) == 0:
            self.log.debug("No tasks to consider for execution.")
            return executable_tis

        # Put one task instance on each line
        task_instance_str = "\n\t".join(
            [repr(x) for x in task_instances_to_examine])
        self.log.info(
            "%s tasks up for execution:\n\t%s", len(task_instances_to_examine),
            task_instance_str
        )

        # Get the pool settings
        pools = {p.pool: p for p in session.query(models.Pool).all()}

        pool_to_task_instances = defaultdict(list)
        for task_instance in task_instances_to_examine:
            pool_to_task_instances[task_instance.pool].append(task_instance)

        # dag_id to # of running tasks and (dag_id, task_id) to # of running tasks.
        dag_concurrency_map, task_concurrency_map = self.__get_concurrency_maps(
            states=list(EXECUTION_STATES), session=session)

        num_tasks_in_executor = 0
        # Number of tasks that cannot be scheduled because of no open slot in pool
        num_starving_tasks_total = 0

        # Go through each pool, and queue up a task for execution if there are
        # any open slots in the pool.
        # pylint: disable=too-many-nested-blocks
        for pool, task_instances in pool_to_task_instances.items():
            pool_name = pool
            if pool not in pools:
                self.log.warning(
                    "Tasks using non-existent pool '%s' will not be scheduled",
                    pool
                )
                continue

            open_slots = pools[pool].open_slots(session=session)

            num_ready = len(task_instances)
            self.log.info(
                "Figuring out tasks to run in Pool(name=%s) with %s open slots "
                "and %s task instances ready to be queued",
                pool, open_slots, num_ready
            )

            priority_sorted_task_instances = sorted(
                task_instances, key=lambda ti: (-ti.priority_weight, ti.execution_date))

            num_starving_tasks = 0
            for current_index, task_instance in enumerate(priority_sorted_task_instances):
                if open_slots <= 0:
                    self.log.info(
                        "Not scheduling since there are %s open slots in pool %s",
                        open_slots, pool
                    )
                    # Can't schedule any more since there are no more open slots.
                    num_starving_tasks = len(priority_sorted_task_instances) - current_index
                    num_starving_tasks_total += num_starving_tasks
                    break

                # Check to make sure that the task concurrency of the DAG hasn't been
                # reached.
                dag_id = task_instance.dag_id
                simple_dag = simple_dag_bag.get_dag(dag_id)

                current_dag_concurrency = dag_concurrency_map[dag_id]
                dag_concurrency_limit = simple_dag_bag.get_dag(dag_id).concurrency
                self.log.info(
                    "DAG %s has %s/%s running and queued tasks",
                    dag_id, current_dag_concurrency, dag_concurrency_limit
                )
                if current_dag_concurrency >= dag_concurrency_limit:
                    self.log.info(
                        "Not executing %s since the number of tasks running or queued "
                        "from DAG %s is >= to the DAG's task concurrency limit of %s",
                        task_instance, dag_id, dag_concurrency_limit
                    )
                    continue

                task_concurrency_limit = simple_dag.get_task_special_arg(
                    task_instance.task_id,
                    'task_concurrency')
                if task_concurrency_limit is not None:
                    current_task_concurrency = task_concurrency_map[
                        (task_instance.dag_id, task_instance.task_id)
                    ]

                    if current_task_concurrency >= task_concurrency_limit:
                        self.log.info("Not executing %s since the task concurrency for"
                                      " this task has been reached.", task_instance)
                        continue

                if self.executor.has_task(task_instance):
                    self.log.debug(
                        "Not handling task %s as the executor reports it is running",
                        task_instance.key
                    )
                    num_tasks_in_executor += 1
                    continue

                executable_tis.append(task_instance)
                open_slots -= 1
                dag_concurrency_map[dag_id] += 1
                task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1

            Stats.gauge(f'pool.starving_tasks.{pool_name}', num_starving_tasks)

        Stats.gauge('scheduler.tasks.starving', num_starving_tasks_total)
        Stats.gauge('scheduler.tasks.running', num_tasks_in_executor)
        Stats.gauge('scheduler.tasks.executable', len(executable_tis))

        task_instance_str = "\n\t".join(
            [repr(x) for x in executable_tis])
        self.log.info(
            "Setting the following tasks to queued state:\n\t%s", task_instance_str)
        # so these dont expire on commit
        for ti in executable_tis:
            copy_dag_id = ti.dag_id
            copy_execution_date = ti.execution_date
            copy_task_id = ti.task_id
            make_transient(ti)
            ti.dag_id = copy_dag_id
            ti.execution_date = copy_execution_date
            ti.task_id = copy_task_id
        return executable_tis

    @provide_session
    def _change_state_for_executable_task_instances(self, task_instances, session=None):
        """
        Changes the state of task instances in the list with one of the given states
        to QUEUED atomically, and returns the TIs changed in SimpleTaskInstance format.

        :param task_instances: TaskInstances to change the state of
        :type task_instances: list[airflow.models.TaskInstance]
        :rtype: list[airflow.models.taskinstance.SimpleTaskInstance]
        """
        if len(task_instances) == 0:
            session.commit()
            return []

        TI = models.TaskInstance

        tis_to_set_to_queued = (
            session
            .query(TI)
            .filter(TI.filter_for_tis(task_instances))
            .filter(TI.state == State.SCHEDULED)
            .with_for_update()
            .all()
        )

        if len(tis_to_set_to_queued) == 0:
            self.log.info("No tasks were able to have their state changed to queued.")
            session.commit()
            return []

        # set TIs to queued state
        filter_for_tis = TI.filter_for_tis(tis_to_set_to_queued)
        session.query(TI).filter(filter_for_tis).update(
            {TI.state: State.QUEUED, TI.queued_dttm: timezone.utcnow()}, synchronize_session=False
        )
        session.commit()

        # Generate a list of SimpleTaskInstance for the use of queuing
        # them in the executor.
        simple_task_instances = [SimpleTaskInstance(ti) for ti in tis_to_set_to_queued]

        task_instance_str = "\n\t".join([repr(x) for x in tis_to_set_to_queued])
        self.log.info("Setting the following %s tasks to queued state:\n\t%s",
                      len(tis_to_set_to_queued), task_instance_str)
        return simple_task_instances

    def _enqueue_task_instances_with_queued_state(self, simple_dag_bag,
                                                  simple_task_instances):
        """
        Takes task_instances, which should have been set to queued, and enqueues them
        with the executor.

        :param simple_task_instances: TaskInstances to enqueue
        :type simple_task_instances: list[SimpleTaskInstance]
        :param simple_dag_bag: Should contains all of the task_instances' dags
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        """
        TI = models.TaskInstance
        # actually enqueue them
        for simple_task_instance in simple_task_instances:
            simple_dag = simple_dag_bag.get_dag(simple_task_instance.dag_id)
            command = TI.generate_command(
                simple_task_instance.dag_id,
                simple_task_instance.task_id,
                simple_task_instance.execution_date,
                local=True,
                mark_success=False,
                ignore_all_deps=False,
                ignore_depends_on_past=False,
                ignore_task_deps=False,
                ignore_ti_state=False,
                pool=simple_task_instance.pool,
                file_path=simple_dag.full_filepath,
                pickle_id=simple_dag.pickle_id)

            priority = simple_task_instance.priority_weight
            queue = simple_task_instance.queue
            self.log.info(
                "Sending %s to executor with priority %s and queue %s",
                simple_task_instance.key, priority, queue
            )

            self.executor.queue_command(
                simple_task_instance,
                command,
                priority=priority,
                queue=queue)

    @provide_session
    def _execute_task_instances(self,
                                simple_dag_bag,
                                session=None):
        """
        Attempts to execute TaskInstances that should be executed by the scheduler.

        There are three steps:
        1. Pick TIs by priority with the constraint that they are in the expected states
        and that we do exceed max_active_runs or pool limits.
        2. Change the state for the TIs above atomically.
        3. Enqueue the TIs in the executor.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        :return: Number of task instance with state changed.
        """
        executable_tis = self._find_executable_task_instances(simple_dag_bag,
                                                              session=session)

        def query(result, items):
            simple_tis_with_state_changed = \
                self._change_state_for_executable_task_instances(items,
                                                                 session=session)
            self._enqueue_task_instances_with_queued_state(
                simple_dag_bag,
                simple_tis_with_state_changed)
            session.commit()
            return result + len(simple_tis_with_state_changed)

        return helpers.reduce_in_chunks(query, executable_tis, 0, self.max_tis_per_query)

    @provide_session
    def _change_state_for_tasks_failed_to_execute(self, session=None):
        """
        If there are tasks left over in the executor,
        we set them back to SCHEDULED to avoid creating hanging tasks.

        :param session: session for ORM operations
        """
        if self.executor.queued_tasks:
            TI = models.TaskInstance
            filter_for_ti_state_change = (
                [and_(
                    TI.dag_id == dag_id,
                    TI.task_id == task_id,
                    TI.execution_date == execution_date,
                    # The TI.try_number will return raw try_number+1 since the
                    # ti is not running. And we need to -1 to match the DB record.
                    TI._try_number == try_number - 1,  # pylint: disable=protected-access
                    TI.state == State.QUEUED)
                    for dag_id, task_id, execution_date, try_number
                    in self.executor.queued_tasks.keys()])
            ti_query = (session.query(TI)
                        .filter(or_(*filter_for_ti_state_change)))
            tis_to_set_to_scheduled = (ti_query
                                       .with_for_update()
                                       .all())
            if len(tis_to_set_to_scheduled) == 0:
                session.commit()
                return

            # set TIs to queued state
            filter_for_tis = TI.filter_for_tis(tis_to_set_to_scheduled)
            session.query(TI).filter(filter_for_tis).update(
                {TI.state: State.SCHEDULED, TI.queued_dttm: None}, synchronize_session=False
            )

            for task_instance in tis_to_set_to_scheduled:
                self.executor.queued_tasks.pop(task_instance.key)

            task_instance_str = "\n\t".join(
                [repr(x) for x in tis_to_set_to_scheduled])

            session.commit()
            self.log.info("Set the following tasks to scheduled state:\n\t%s", task_instance_str)

    @provide_session
    def _process_executor_events(self, simple_dag_bag, session=None):
        """
        Respond to executor events.
        """
        # TODO: this shares quite a lot of code with _manage_executor_state

        TI = models.TaskInstance
        # pylint: disable=too-many-nested-blocks
        for key, state in list(self.executor.get_event_buffer(simple_dag_bag.dag_ids)
                                   .items()):
            dag_id, task_id, execution_date, try_number = key
            self.log.info(
                "Executor reports execution of %s.%s execution_date=%s "
                "exited with status %s for try_number %s",
                dag_id, task_id, execution_date, state, try_number
            )
            if state in (State.FAILED, State.SUCCESS):
                qry = session.query(TI).filter(TI.dag_id == dag_id,
                                               TI.task_id == task_id,
                                               TI.execution_date == execution_date)
                ti = qry.first()
                if not ti:
                    self.log.warning("TaskInstance %s went missing from the database", ti)
                    continue

                # TODO: should we fail RUNNING as well, as we do in Backfills?
                if ti.try_number == try_number and ti.state == State.QUEUED:
                    Stats.incr('scheduler.tasks.killed_externally')
                    self.log.error(
                        "Executor reports task instance %s finished (%s) although the task says its %s. "
                        "Was the task killed externally?",
                        ti, state, ti.state
                    )
                    simple_dag = simple_dag_bag.get_dag(dag_id)
                    self.processor_agent.send_callback_to_execute(
                        full_filepath=simple_dag.full_filepath,
                        task_instance=ti,
                        msg="Executor reports task instance finished ({}) although the task says its {}. "
                            "Was the task killed externally?".format(state, ti.state)
                    )

    def _execute(self):
        self.log.info("Starting the scheduler")

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = False
        if self.do_pickle and self.executor.__class__ not in (LocalExecutor, SequentialExecutor):
            pickle_dags = True

        self.log.info("Processing each file at most %s times", self.num_runs)

        def processor_factory(file_path, failure_callback_requests):
            return DagFileProcessorProcess(
                file_path=file_path,
                pickle_dags=pickle_dags,
                dag_id_white_list=self.dag_ids,
                failure_callback_requests=failure_callback_requests
            )

        # When using sqlite, we do not use async_mode
        # so the scheduler job and DAG parser don't access the DB at the same time.
        async_mode = not self.using_sqlite

        processor_timeout_seconds = conf.getint('core', 'dag_file_processor_timeout')
        processor_timeout = timedelta(seconds=processor_timeout_seconds)
        self.processor_agent = DagFileProcessorAgent(self.subdir,
                                                     self.num_runs,
                                                     processor_factory,
                                                     processor_timeout,
                                                     async_mode)

        try:
            self.executor.start()

            self.log.info("Resetting orphaned tasks for active dag runs")
            self.reset_state_for_orphaned_tasks()

            self.register_exit_signals()

            # Start after resetting orphaned tasks to avoid stressing out DB.
            self.processor_agent.start()

            execute_start_time = timezone.utcnow()

            self._run_scheduler_loop()

            # Stop any processors
            self.processor_agent.terminate()

            # Verify that all files were processed, and if so, deactivate DAGs that
            # haven't been touched by the scheduler as they likely have been
            # deleted.
            if self.processor_agent.all_files_processed:
                self.log.info(
                    "Deactivating DAGs that haven't been touched since %s",
                    execute_start_time.isoformat()
                )
                models.DAG.deactivate_stale_dags(execute_start_time)

            self.executor.end()

            settings.Session.remove()
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Exception when executing execute_helper")
        finally:
            self.processor_agent.end()
            self.log.info("Exited execute loop")

    def _run_scheduler_loop(self):
        """
        The actual scheduler loop. The main steps in the loop are:
            #. Harvest DAG parsing results through DagFileProcessorAgent
            #. Find and queue executable tasks
                #. Change task instance state in DB
                #. Queue tasks in executor
            #. Heartbeat executor
                #. Execute queued tasks in executor asynchronously
                #. Sync on the states of running tasks

        Following is a graphic representation of these steps.

        .. image:: ../docs/img/scheduler_loop.jpg

        :rtype: None
        """
        # Last time that self.heartbeat() was called.
        last_self_heartbeat_time = timezone.utcnow()

        is_unit_test = conf.getboolean('core', 'unit_test_mode')

        # For the execute duration, parse and schedule DAGs
        while True:
            loop_start_time = time.time()

            if self.using_sqlite:
                self.processor_agent.run_single_parsing_loop()
                # For the sqlite case w/ 1 thread, wait until the processor
                # is finished to avoid concurrent access to the DB.
                self.log.debug("Waiting for processors to finish since we're using sqlite")
                self.processor_agent.wait_until_finished()

            simple_dags = self.processor_agent.harvest_simple_dags()

            self.log.debug("Harvested %d SimpleDAGs", len(simple_dags))

            # Send tasks for execution if available
            simple_dag_bag = SimpleDagBag(simple_dags)

            if not self._validate_and_run_task_instances(simple_dag_bag=simple_dag_bag):
                continue

            # Heartbeat the scheduler periodically
            time_since_last_heartbeat = (timezone.utcnow() -
                                         last_self_heartbeat_time).total_seconds()
            if time_since_last_heartbeat > self.heartrate:
                self.log.debug("Heartbeating the scheduler")
                self.heartbeat()
                last_self_heartbeat_time = timezone.utcnow()

            self._emit_pool_metrics()

            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            self.log.debug("Ran scheduling loop in %.2f seconds", loop_duration)

            if not is_unit_test:
                time.sleep(self._processor_poll_interval)

            if self.processor_agent.done:
                self.log.info("Exiting scheduler loop as all files have been processed %d times",
                              self.num_runs)
                break

    def _validate_and_run_task_instances(self, simple_dag_bag: SimpleDagBag) -> bool:
        if len(simple_dag_bag.simple_dags) > 0:
            try:
                self._process_and_execute_tasks(simple_dag_bag)
            except Exception as e:  # pylint: disable=broad-except
                self.log.error("Error queuing tasks")
                self.log.exception(e)
                return False

        # Call heartbeats
        self.log.debug("Heartbeating the executor")
        self.executor.heartbeat()

        self._change_state_for_tasks_failed_to_execute()

        # Process events from the executor
        self._process_executor_events(simple_dag_bag)
        return True

    def _process_and_execute_tasks(self, simple_dag_bag):
        # Handle cases where a DAG run state is set (perhaps manually) to
        # a non-running state. Handle task instances that belong to
        # DAG runs in those states
        # If a task instance is up for retry but the corresponding DAG run
        # isn't running, mark the task instance as FAILED so we don't try
        # to re-run it.
        self._change_state_for_tis_without_dagrun(simple_dag_bag,
                                                  [State.UP_FOR_RETRY],
                                                  State.FAILED)
        # If a task instance is scheduled or queued or up for reschedule,
        # but the corresponding DAG run isn't running, set the state to
        # NONE so we don't try to re-run it.
        self._change_state_for_tis_without_dagrun(simple_dag_bag,
                                                  [State.QUEUED,
                                                   State.SCHEDULED,
                                                   State.UP_FOR_RESCHEDULE],
                                                  State.NONE)
        self._execute_task_instances(simple_dag_bag)

    @provide_session
    def _emit_pool_metrics(self, session=None) -> None:
        pools = models.Pool.slots_stats(session)
        for pool_name, slot_stats in pools.items():
            Stats.gauge(f'pool.open_slots.{pool_name}', slot_stats["open"])
            Stats.gauge(f'pool.queued_slots.{pool_name}', slot_stats[State.QUEUED])
            Stats.gauge(f'pool.running_slots.{pool_name}', slot_stats[State.RUNNING])

    @provide_session
    def heartbeat_callback(self, session=None):
        Stats.incr('scheduler_heartbeat', 1, 1)
```

## Dag状态显示

### 模型类创建

基于airflow底层框架，在airflow\models中定义Dag模型类，如下面模型类：

```python
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym
from sqlalchemy.orm.session import Session

from airflow.exceptions import AirflowException
from airflow.models.base import ID_LEN, Base
from airflow.models.taskinstance import TaskInstance as TI
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_states import SCHEDULEABLE_STATES
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from airflow.utils.types import DagRunType


class DagRun(Base, LoggingMixin):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __tablename__ = "dag_run"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    execution_date = Column(UtcDateTime, default=timezone.utcnow)
    start_date = Column(UtcDateTime, default=timezone.utcnow)
    end_date = Column(UtcDateTime)
    _state = Column('state', String(50), default=State.RUNNING)
    run_id = Column(String(ID_LEN))
    external_trigger = Column(Boolean, default=True)
    conf = Column(PickleType)

    dag = None

    __table_args__ = (
        Index('dag_id_state', dag_id, _state),
        UniqueConstraint('dag_id', 'execution_date'),
        UniqueConstraint('dag_id', 'run_id'),
    )

    def __init__(self, dag_id=None, run_id=None, execution_date=None, start_date=None, external_trigger=None,
                 conf=None, state=None):
        self.dag_id = dag_id
        self.run_id = run_id
        self.execution_date = execution_date
        self.start_date = start_date
        self.external_trigger = external_trigger
        self.conf = conf
        self.state = state
        super().__init__()

    def __repr__(self):
        return (
            '<DagRun {dag_id} @ {execution_date}: {run_id}, '
            'externally triggered: {external_trigger}>'
        ).format(
            dag_id=self.dag_id,
            execution_date=self.execution_date,
            run_id=self.run_id,
            external_trigger=self.external_trigger)

    def get_state(self):
        return self._state

    def set_state(self, state):
        if self._state != state:
            self._state = state
            self.end_date = timezone.utcnow() if self._state in State.finished() else None

    @declared_attr
    def state(self):
        return synonym('_state', descriptor=property(self.get_state, self.set_state))

    @provide_session
    def refresh_from_db(self, session=None):
        """
        Reloads the current dagrun from the database

        :param session: database session
        """
        DR = DagRun

        exec_date = func.cast(self.execution_date, DateTime)

        dr = session.query(DR).filter(
            DR.dag_id == self.dag_id,
            func.cast(DR.execution_date, DateTime) == exec_date,
            DR.run_id == self.run_id
        ).one()

        self.id = dr.id
        self.state = dr.state

    @staticmethod
    @provide_session
    def find(
        dag_id: Optional[Union[str, List[str]]] = None,
        run_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        state: Optional[str] = None,
        external_trigger: Optional[bool] = None,
        no_backfills: Optional[bool] = False,
        session: Session = None,
        execution_start_date=None, execution_end_date=None
    ):
        """
        Returns a set of dag runs for the given search criteria.

        :param dag_id: the dag_id or list of dag_id to find dag runs for
        :type dag_id: str or list[str]
        :param run_id: defines the run id for this dag run
        :type run_id: str
        :param execution_date: the execution date
        :type execution_date: datetime.datetime or list[datetime.datetime]
        :param state: the state of the dag run
        :type state: str
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param no_backfills: return no backfills (True), return all (False).
            Defaults to False
        :type no_backfills: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param execution_start_date: dag run that was executed from this date
        :type execution_start_date: datetime.datetime
        :param execution_end_date: dag run that was executed until this date
        :type execution_end_date: datetime.datetime
        """
        DR = DagRun

        qry = session.query(DR)
        dag_ids = [dag_id] if isinstance(dag_id, str) else dag_id
        if dag_ids:
            qry = qry.filter(DR.dag_id.in_(dag_ids))
        if run_id:
            qry = qry.filter(DR.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                qry = qry.filter(DR.execution_date.in_(execution_date))
            else:
                qry = qry.filter(DR.execution_date == execution_date)
        if execution_start_date and execution_end_date:
            qry = qry.filter(DR.execution_date.between(execution_start_date, execution_end_date))
        elif execution_start_date:
            qry = qry.filter(DR.execution_date >= execution_start_date)
        elif execution_end_date:
            qry = qry.filter(DR.execution_date <= execution_end_date)
        if state:
            qry = qry.filter(DR.state == state)
        if external_trigger is not None:
            qry = qry.filter(DR.external_trigger == external_trigger)
        if no_backfills:
            # in order to prevent a circular dependency
            qry = qry.filter(DR.run_id.notlike(f"{DagRunType.BACKFILL_JOB.value}__%"))

        dr = qry.order_by(DR.execution_date).all()

        return dr

    @provide_session
    def get_task_instances(self, state=None, session=None):
        """
        Returns the task instances for this dag run
        """
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
        )

        if state:
            if isinstance(state, str):
                tis = tis.filter(TI.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.filter(TI.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.filter(
                            or_(TI.state.in_(not_none_state),
                                TI.state.is_(None))
                        )
                else:
                    tis = tis.filter(TI.state.in_(state))

        if self.dag and self.dag.partial:
            tis = tis.filter(TI.task_id.in_(self.dag.task_ids))
        return tis.all()

    @provide_session
    def get_task_instance(self, task_id, session=None):
        """
        Returns the task instance specified by task_id for this dag run

        :param task_id: the task id
        """
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
            TI.task_id == task_id
        ).first()

        return ti

    def get_dag(self):
        """
        Returns the Dag associated with this DagRun.

        :return: DAG
        """
        if not self.dag:
            raise AirflowException("The DAG (.dag) for {} needs to be set"
                                   .format(self))

        return self.dag

    @provide_session
    def get_previous_dagrun(self, state: Optional[str] = None, session: Session = None) -> Optional['DagRun']:
        """The previous DagRun, if there is one"""

        session = cast(Session, session)  # mypy

        filters = [
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date < self.execution_date,
        ]
        if state is not None:
            filters.append(DagRun.state == state)
        return session.query(DagRun).filter(
            *filters
        ).order_by(
            DagRun.execution_date.desc()
        ).first()

    @provide_session
    def get_previous_scheduled_dagrun(self, session=None):
        """The previous, SCHEDULED DagRun, if there is one"""
        dag = self.get_dag()

        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == dag.previous_schedule(self.execution_date)
        ).first()

    @provide_session
    def update_state(self, session=None):
        """
        Determines the overall state of the DagRun based on the state
        of its TaskInstances.

        :return: ready_tis: the tis that can be scheduled in the current loop
        :rtype ready_tis: list[airflow.models.TaskInstance]
        """

        dag = self.get_dag()
        ready_tis = []
        tis = [ti for ti in self.get_task_instances(session=session,
                                                    state=State.task_states + (State.SHUTDOWN,))]
        self.log.debug("number of tis tasks for %s: %s task(s)", self, len(tis))
        for ti in tis:
            ti.task = dag.get_task(ti.task_id)

        start_dttm = timezone.utcnow()
        unfinished_tasks = [t for t in tis if t.state in State.unfinished()]
        finished_tasks = [t for t in tis if t.state in State.finished() + [State.UPSTREAM_FAILED]]
        none_depends_on_past = all(not t.task.depends_on_past for t in unfinished_tasks)
        none_task_concurrency = all(t.task.task_concurrency is None
                                    for t in unfinished_tasks)
        if unfinished_tasks:
            scheduleable_tasks = [ut for ut in unfinished_tasks if ut.state in SCHEDULEABLE_STATES]
            if none_depends_on_past and none_task_concurrency:
                # small speed up
                self.log.debug(
                    "number of scheduleable tasks for %s: %s task(s)",
                    self, len(scheduleable_tasks))
                ready_tis, changed_tis = self._get_ready_tis(scheduleable_tasks, finished_tasks, session)
                self.log.debug("ready tis length for %s: %s task(s)", self, len(ready_tis))
                are_runnable_tasks = ready_tis or self._are_premature_tis(
                    unfinished_tasks, finished_tasks, session) or changed_tis
            else:
                # slow path
                for ti in scheduleable_tasks:
                    if ti.are_dependencies_met(
                        dep_context=DepContext(flag_upstream_failed=True),
                        session=session
                    ):
                        self.log.debug('Queuing task: %s', ti)
                        ready_tis.append(ti)

        duration = (timezone.utcnow() - start_dttm)
        Stats.timing("dagrun.dependency-check.{}".format(self.dag_id), duration)

        leaf_tis = [ti for ti in tis if ti.task_id in {t.task_id for t in dag.leaves}]

        # if all roots finished and at least one failed, the run failed
        if not unfinished_tasks and any(
            leaf_ti.state in {State.FAILED, State.UPSTREAM_FAILED} for leaf_ti in leaf_tis
        ):
            self.log.error('Marking run %s failed', self)
            self.set_state(State.FAILED)
            dag.handle_callback(self, success=False, reason='task_failure',
                                session=session)

        # if all leafs succeeded and no unfinished tasks, the run succeeded
        elif not unfinished_tasks and all(
            leaf_ti.state in {State.SUCCESS, State.SKIPPED} for leaf_ti in leaf_tis
        ):
            self.log.info('Marking run %s successful', self)
            self.set_state(State.SUCCESS)
            dag.handle_callback(self, success=True, reason='success', session=session)

        # if *all tasks* are deadlocked, the run failed
        elif (unfinished_tasks and none_depends_on_past and
              none_task_concurrency and not are_runnable_tasks):
            self.log.error('Deadlock; marking run %s failed', self)
            self.set_state(State.FAILED)
            dag.handle_callback(self, success=False, reason='all_tasks_deadlocked',
                                session=session)

        # finally, if the roots aren't done, the dag is still running
        else:
            self.set_state(State.RUNNING)

        self._emit_duration_stats_for_finished_state()

        # todo: determine we want to use with_for_update to make sure to lock the run
        session.merge(self)
        session.commit()

        return ready_tis

    def _get_ready_tis(
        self,
        scheduleable_tasks: List[TI],
        finished_tasks: List[TI],
        session: Session,
    ) -> Tuple[List[TI], bool]:
        old_states = {}
        ready_tis: List[TI] = []
        changed_tis = False

        if not scheduleable_tasks:
            return ready_tis, changed_tis

        # Check dependencies
        for st in scheduleable_tasks:
            old_state = st.state
            if st.are_dependencies_met(
                dep_context=DepContext(
                    flag_upstream_failed=True,
                    finished_tasks=finished_tasks),
                    session=session):
                ready_tis.append(st)
            else:
                old_states[st.key] = old_state

        # Check if any ti changed state
        tis_filter = TI.filter_for_tis(old_states.keys())
        if tis_filter is not None:
            fresh_tis = session.query(TI).filter(tis_filter).all()
            changed_tis = any(ti.state != old_states[ti.key] for ti in fresh_tis)

        return ready_tis, changed_tis

    def _are_premature_tis(
        self,
        unfinished_tasks: List[TI],
        finished_tasks: List[TI],
        session: Session,
    ) -> bool:
        # there might be runnable tasks that are up for retry and for some reason(retry delay, etc) are
        # not ready yet so we set the flags to count them in
        for ut in unfinished_tasks:
            if ut.are_dependencies_met(
                dep_context=DepContext(
                    flag_upstream_failed=True,
                    ignore_in_retry_period=True,
                    ignore_in_reschedule_period=True,
                    finished_tasks=finished_tasks),
                    session=session):
                return True
        return False

    def _emit_duration_stats_for_finished_state(self):
        if self.state == State.RUNNING:
            return

        duration = (self.end_date - self.start_date)
        if self.state is State.SUCCESS:
            Stats.timing('dagrun.duration.success.{}'.format(self.dag_id), duration)
        elif self.state == State.FAILED:
            Stats.timing('dagrun.duration.failed.{}'.format(self.dag_id), duration)

    @provide_session
    def verify_integrity(self, session=None):
        """
        Verifies the DagRun by checking for removed tasks or tasks that are not in the
        database yet. It will set state to removed or add the task if required.
        """
        dag = self.get_dag()
        tis = self.get_task_instances(session=session)

        # check for removed or restored tasks
        task_ids = []
        for ti in tis:
            task_ids.append(ti.task_id)
            task = None
            try:
                task = dag.get_task(ti.task_id)
            except AirflowException:
                if ti.state == State.REMOVED:
                    pass  # ti has already been removed, just ignore it
                elif self.state is not State.RUNNING and not dag.partial:
                    self.log.warning("Failed to get task '{}' for dag '{}'. "
                                     "Marking it as removed.".format(ti, dag))
                    Stats.incr(
                        "task_removed_from_dag.{}".format(dag.dag_id), 1, 1)
                    ti.state = State.REMOVED

            should_restore_task = (task is not None) and ti.state == State.REMOVED
            if should_restore_task:
                self.log.info("Restoring task '{}' which was previously "
                              "removed from DAG '{}'".format(ti, dag))
                Stats.incr("task_restored_to_dag.{}".format(dag.dag_id), 1, 1)
                ti.state = State.NONE

        # check for missing tasks
        for task in dag.task_dict.values():
            if task.start_date > self.execution_date and not self.is_backfill:
                continue

            if task.task_id not in task_ids:
                Stats.incr(
                    "task_instance_created-{}".format(task.__class__.__name__),
                    1, 1)
                ti = TI(task, self.execution_date)
                session.add(ti)

        session.commit()

    @staticmethod
    def get_run(session, dag_id, execution_date):
        """
        :param dag_id: DAG ID
        :type dag_id: unicode
        :param execution_date: execution date
        :type execution_date: datetime
        :return: DagRun corresponding to the given dag_id and execution date
            if one exists. None otherwise.
        :rtype: airflow.models.DagRun
        """
        qry = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.external_trigger == False,  # noqa pylint: disable=singleton-comparison
            DagRun.execution_date == execution_date,
        )
        return qry.first()

    @property
    def is_backfill(self):
        return (
            self.run_id is not None and
            self.run_id.startswith(f"{DagRunType.BACKFILL_JOB.value}")
        )

    @classmethod
    @provide_session
    def get_latest_runs(cls, session):
        """Returns the latest DagRun for each DAG. """
        subquery = (
            session
            .query(
                cls.dag_id,
                func.max(cls.execution_date).label('execution_date'))
            .group_by(cls.dag_id)
            .subquery()
        )
        dagruns = (
            session
            .query(cls)
            .join(subquery,
                  and_(cls.dag_id == subquery.c.dag_id,
                       cls.execution_date == subquery.c.execution_date))
            .all()
        )
        return dagruns

```

###　Dag状态显示接口

可以在airflow\dag中编写Dag 状态显示api视图接口,包括所需的后端逻辑方法

如下面逻辑接口代码：

get_dag_run_state.py

```python
"""DAG run APIs."""
from datetime import datetime
from typing import Dict

from airflow.api.common.experimental import check_and_get_dag, check_and_get_dagrun


def get_dag_run_state(dag_id: str, execution_date: datetime) -> Dict[str, str]:
    """Return the task object identified by the given dag_id and task_id.

    :param dag_id: DAG id
    :param execution_date: execution date
    :return: Dictionary storing state of the object
    """

    dag = check_and_get_dag(dag_id=dag_id)

    dagrun = check_and_get_dagrun(dag, execution_date)

    return {'state': dagrun.get_state()}
```

## Task状态显示

### 模型类创建

基于airflow底层框架，在airflow\models中定义Task模型类,如下面模型类：

```python
import copy
import getpass
import hashlibache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import math
import os
import signal
import time
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import quote

import dill
import lazy_object_proxy
import pendulum
from jinja2 import TemplateAssertionError, UndefinedError
from sqlalchemy import Column, Float, Index, Integer, PickleType, String, and_, func, or_
from sqlalchemy.orm import reconstructor
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.elements import BooleanClauseList

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException, AirflowRescheduleException, AirflowSkipException, AirflowTaskTimeout,
)
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.models.log import Log
from airflow.models.taskfail import TaskFail
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.variable import Variable
from airflow.models.xcom import XCOM_RETURN_KEY, XCom
from airflow.sentry import Sentry
from airflow.settings import STORE_SERIALIZED_DAGS
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.utils import timezone
from airflow.utils.email import send_email
from airflow.utils.helpers import is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from airflow.utils.timeout import timeout


def clear_task_instances(tis,
                         session,
                         activate_dag_runs=True,
                         dag=None,
                         ):
    """
    Clears a set of task instances, but makes sure the running ones
    get killed.

    :param tis: a list of task instances
    :param session: current session
    :param activate_dag_runs: flag to check for active dag run
    :param dag: DAG object
    """
    job_ids = []
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                task = dag.get_task(task_id)
                ti.refresh_from_task(task)
                task_retries = task.retries
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the last attempted try number.
                ti.max_tries = max(ti.max_tries, ti.prev_attempted_tries)
            ti.state = State.NONE
            session.merge(ti)
        # Clear all reschedules related to the ti to clear
        TR = TaskReschedule
        session.query(TR).filter(
            TR.dag_id == ti.dag_id,
            TR.task_id == ti.task_id,
            TR.execution_date == ti.execution_date,
            TR.try_number == ti.try_number
        ).delete()

    if job_ids:
        from airflow.jobs.base_job import BaseJob as BJ
        for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
            job.state = State.SHUTDOWN

    if activate_dag_runs and tis:
        from airflow.models.dagrun import DagRun  # Avoid circular import
        drs = session.query(DagRun).filter(
            DagRun.dag_id.in_({ti.dag_id for ti in tis}),
            DagRun.execution_date.in_({ti.execution_date for ti in tis}),
        ).all()
        for dr in drs:
            dr.state = State.RUNNING
            dr.start_date = timezone.utcnow()


# Key used to identify task instance
# Tuple of: dag_id, task_id, execution_date, try_number
TaskInstanceKeyType = Tuple[str, str, datetime, int]


class TaskInstance(Base, LoggingMixin):
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50), nullable=False)
    pool_slots = Column(Integer, default=1)
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))
    # If adding new fields here then remember to add them to
    # refresh_from_db() or they wont display in the UI correctly

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_dag_date', dag_id, execution_date),
        Index('ti_state', state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
        Index('ti_pool', pool, state, priority_weight),
        Index('ti_job_id', job_id),
    )

    def __init__(self, task, execution_date: datetime, state: Optional[str] = None):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.task = task
        self.refresh_from_task(task)
        self._log = logging.getLogger("airflow.task")

        # make sure we have a localized execution_date stored in UTC
        if execution_date and not timezone.is_localized(execution_date):
            self.log.warning("execution date %s has no timezone information. Using "
                             "default from dag or system", execution_date)
            if self.task.has_dag():
                execution_date = timezone.make_aware(execution_date,
                                                     self.task.dag.timezone)
            else:
                execution_date = timezone.make_aware(execution_date)

            execution_date = timezone.convert_to_utc(execution_date)

        self.execution_date = execution_date

        self.try_number = 0
        self.unixname = getpass.getuser()
        if state:
            self.state = state
        self.hostname = ''
        self.init_on_load()
        # Is this TaskInstance being currently running within `airflow tasks run --raw`.
        # Not persisted to the database so only valid for the current process
        self.raw = False

    @reconstructor
    def init_on_load(self):
        """ Initialize the attributes that aren't stored in the DB. """
        self.test_mode = False  # can be changed when calling 'run'

    @property
    def try_number(self):
        """
        Return the try number that this task number will be when it is actually
        run.

        If the TaskInstance is currently running, this will match the column in the
        database, in all other cases this will be incremented.
        """
        # This is designed so that task logs end up in the right file.
        if self.state == State.RUNNING:
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value):
        self._try_number = value

    @property
    def prev_attempted_tries(self):
        """
        Based on this instance's try_number, this will calculate
        the number of previously attempted tries, defaulting to 0.
        """
        # Expose this for the Task Tries and Gantt graph views.
        # Using `try_number` throws off the counts for non-running tasks.
        # Also useful in error logging contexts to get
        # the try number for the last try that was attempted.
        # https://issues.apache.org/jira/browse/AIRFLOW-2143

        return self._try_number

    @property
    def next_try_number(self):
        return self._try_number + 1

    def command_as_list(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_task_deps=False,
            ignore_depends_on_past=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        dag = self.task.dag

        should_pass_filepath = not pickle_id and dag
        if should_pass_filepath and dag.full_filepath != dag.filepath:
            path = "DAGS_FOLDER/{}".format(dag.filepath)
        elif should_pass_filepath and dag.full_filepath:
            path = dag.full_filepath
        else:
            path = None

        return TaskInstance.generate_command(
            self.dag_id,
            self.task_id,
            self.execution_date,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            file_path=path,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path)

    @staticmethod
    def generate_command(dag_id: str,
                         task_id: str,
                         execution_date: datetime,
                         mark_success: Optional[bool] = False,
                         ignore_all_deps: Optional[bool] = False,
                         ignore_depends_on_past: Optional[bool] = False,
                         ignore_task_deps: Optional[bool] = False,
                         ignore_ti_state: Optional[bool] = False,
                         local: Optional[bool] = False,
                         pickle_id: Optional[str] = None,
                         file_path: Optional[str] = None,
                         raw: Optional[bool] = False,
                         job_id: Optional[str] = None,
                         pool: Optional[str] = None,
                         cfg_path: Optional[str] = None
                         ) -> List[str]:
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :type dag_id: str
        :param task_id: Task ID
        :type task_id: str
        :param execution_date: Execution date for the task
        :type execution_date: datetime
        :param mark_success: Whether to mark the task as successful
        :type mark_success: Optional[bool]
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :type ignore_all_deps: Optional[bool]
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :type ignore_depends_on_past: Optional[bool]
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :type ignore_task_deps: Optional[bool]
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :type ignore_ti_state: Optional[bool]
        :param local: Whether to run the task locally
        :type local: Optional[bool]
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :type pickle_id: Optional[str]
        :param file_path: path to the file containing the DAG definition
        :type file_path: Optional[str]
        :param raw: raw mode (needs more details)
        :type raw: Optional[bool]
        :param job_id: job ID (needs more details)
        :type job_id: Optional[int]
        :param pool: the Airflow pool that the task should run in
        :type pool: Optional[str]
        :param cfg_path: the Path to the configuration file
        :type cfg_path: Optional[str]
        :return: shell command that can be used to run the task instance
        :rtype: list[str]
        """
        iso = execution_date.isoformat()
        cmd = ["airflow", "tasks", "run", dag_id, task_id, iso]
        if mark_success:
            cmd.extend(["--mark-success"])
        if pickle_id:
            cmd.extend(["--pickle", pickle_id])
        if job_id:
            cmd.extend(["--job-id", str(job_id)])
        if ignore_all_deps:
            cmd.extend(["--ignore-all-dependencies"])
        if ignore_task_deps:
            cmd.extend(["--ignore-dependencies"])
        if ignore_depends_on_past:
            cmd.extend(["--ignore-depends-on-past"])
        if ignore_ti_state:
            cmd.extend(["--force"])
        if local:
            cmd.extend(["--local"])
        if pool:
            cmd.extend(["--pool", pool])
        if raw:
            cmd.extend(["--raw"])
        if file_path:
            cmd.extend(["--subdir", file_path])
        if cfg_path:
            cmd.extend(["--cfg-path", cfg_path])
        return cmd

    @property
    def log_filepath(self):
        iso = self.execution_date.isoformat()
        log = os.path.expanduser(conf.get('logging', 'BASE_LOG_FOLDER'))
        return ("{log}/{dag_id}/{task_id}/{iso}.log".format(
            log=log, dag_id=self.dag_id, task_id=self.task_id, iso=iso))

    @property
    def log_url(self):
        iso = quote(self.execution_date.isoformat())
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/log?"
            "execution_date={iso}"
            "&task_id={task_id}"
            "&dag_id={dag_id}"
        ).format(iso=iso, task_id=self.task_id, dag_id=self.dag_id)

    @property
    def mark_success_url(self):
        iso = quote(self.execution_date.isoformat())
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/success"
            "?task_id={task_id}"
            "&dag_id={dag_id}"
            "&execution_date={iso}"
            "&upstream=false"
            "&downstream=false"
        ).format(task_id=self.task_id, dag_id=self.dag_id, iso=iso)

    @provide_session
    def current_state(self, session=None) -> str:
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.
        """
        ti = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.execution_date == self.execution_date,
        ).all()
        if ti:
            state = ti[0].state
        else:
            state = None
        return state

    @provide_session
    def error(self, session=None):
        """
        Forces the task instance's state to FAILED in the database.
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()

    @provide_session
    def refresh_from_db(self, session=None, lock_for_update=False) -> None:
        """
        Refreshes the task instance from the database based on the primary key

        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """

        qry = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.execution_date == self.execution_date)

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        if ti:
            # Fields ordered per model definition
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            self.duration = ti.duration
            self.state = ti.state
            # Get the raw value of try_number column, don't read through the
            # accessor here otherwise it will be incremented by one already.
            self.try_number = ti._try_number
            self.max_tries = ti.max_tries
            self.hostname = ti.hostname
            self.unixname = ti.unixname
            self.job_id = ti.job_id
            self.pool = ti.pool
            self.pool_slots = ti.pool_slots or 1
            self.queue = ti.queue
            self.priority_weight = ti.priority_weight
            self.operator = ti.operator
            self.queued_dttm = ti.queued_dttm
            self.pid = ti.pid
        else:
            self.state = None

    def refresh_from_task(self, task, pool_override=None):
        """
        Copy common attributes from the given task.

        :param task: The task object to copy from
        :type task: airflow.models.BaseOperator
        :param pool_override: Use the pool_override instead of task's pool
        :type pool_override: str
        """
        self.queue = task.queue
        self.pool = pool_override or task.pool
        self.pool_slots = task.pool_slots
        self.priority_weight = task.priority_weight_total
        self.run_as_user = task.run_as_user
        self.max_tries = task.retries
        self.executor_config = task.executor_config
        self.operator = task.__class__.__name__

    @provide_session
    def clear_xcom_data(self, session=None):
        """
        Clears all XCom data from the database for the task instance
        """
        session.query(XCom).filter(
            XCom.dag_id == self.dag_id,
            XCom.task_id == self.task_id,
            XCom.execution_date == self.execution_date
        ).delete()
        session.commit()

    @property
    def key(self) -> TaskInstanceKeyType:
        """
        Returns a tuple that identifies the task instance uniquely
        """
        return self.dag_id, self.task_id, self.execution_date, self.try_number

    @provide_session
    def set_state(self, state, session=None, commit=True):
        self.state = state
        self.start_date = timezone.utcnow()
        self.end_date = timezone.utcnow()
        session.merge(self)
        if commit:
            session.commit()

    @property
    def is_premature(self):
        """
        Returns whether a task is in UP_FOR_RETRY state and its retry interval
        has elapsed.
        """
        # is the task still in the retry waiting period?
        return self.state == State.UP_FOR_RETRY and not self.ready_for_retry()

    @provide_session
    def are_dependents_done(self, session=None):
        """
        Checks whether the dependents of this task instance have all succeeded.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.
        """
        task = self.task

        if not task.downstream_task_ids:
            return True

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state == State.SUCCESS,
        )
        count = ti[0][0]
        return count == len(task.downstream_task_ids)

    @provide_session
    def get_previous_ti(
        self,
        state: Optional[str] = None,
        session: Session = None
    ) -> Optional['TaskInstance']:
        """
        The task instance for the task that ran before this task instance.

        :param state: If passed, it only take into account instances of a specific state.
        """
        dag = self.task.dag
        if dag:
            dr = self.get_dagrun(session=session)

            # LEGACY: most likely running from unit tests
            if not dr:
                # Means that this TaskInstance is NOT being run from a DR, but from a catchup
                previous_scheduled_date = dag.previous_schedule(self.execution_date)
                if not previous_scheduled_date:
                    return None

                return TaskInstance(task=self.task, execution_date=previous_scheduled_date)

            dr.dag = dag

            # We always ignore schedule in dagrun lookup when `state` is given or `schedule_interval is None`.
            # For legacy reasons, when `catchup=True`, we use `get_previous_scheduled_dagrun` unless
            # `ignore_schedule` is `True`.
            ignore_schedule = state is not None or dag.schedule_interval is None
            if dag.catchup is True and not ignore_schedule:
                last_dagrun = dr.get_previous_scheduled_dagrun(session=session)
            else:
                last_dagrun = dr.get_previous_dagrun(session=session, state=state)

            if last_dagrun:
                return last_dagrun.get_task_instance(self.task_id, session=session)

        return None

    @property
    def previous_ti(self):
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_ti()

    @property
    def previous_ti_success(self) -> Optional['TaskInstance']:
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_ti(state=State.SUCCESS)

    @provide_session
    def get_previous_execution_date(
        self,
        state: Optional[str] = None,
        session: Session = None,
    ) -> Optional[pendulum.datetime]:
        """
        The execution date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        """
        self.log.debug("previous_execution_date was called")
        prev_ti = self.get_previous_ti(state=state, session=session)
        return prev_ti and prev_ti.execution_date

    @provide_session
    def get_previous_start_date(
        self,
        state: Optional[str] = None,
        session: Session = None
    ) -> Optional[pendulum.datetime]:
        """
        The start date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        """
        self.log.debug("previous_start_date was called")
        prev_ti = self.get_previous_ti(state=state, session=session)
        return prev_ti and prev_ti.start_date

    @property
    def previous_start_date_success(self) -> Optional[pendulum.datetime]:
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_start_date` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_start_date` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_start_date(state=State.SUCCESS)

    @provide_session
    def are_dependencies_met(
            self,
            dep_context=None,
            session=None,
            verbose=False):
        """
        Returns whether or not all the conditions are met for this task instance to be run
        given the context for the dependencies (e.g. a task instance being force run from
        the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that
            should be evaluated.
        :type dep_context: DepContext
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param verbose: whether log details on failed dependencies on
            info or debug log level
        :type verbose: bool
        """
        dep_context = dep_context or DepContext()
        failed = False
        verbose_aware_logger = self.log.info if verbose else self.log.debug
        for dep_status in self.get_failed_dep_statuses(
                dep_context=dep_context,
                session=session):
            failed = True

            verbose_aware_logger(
                "Dependencies not met for %s, dependency '%s' FAILED: %s",
                self, dep_status.dep_name, dep_status.reason
            )

        if failed:
            return False

        verbose_aware_logger("Dependencies all met for %s", self)
        return True

    @provide_session
    def get_failed_dep_statuses(
            self,
            dep_context=None,
            session=None):
        dep_context = dep_context or DepContext()
        for dep in dep_context.deps | self.task.deps:
            for dep_status in dep.get_dep_statuses(
                    self,
                    session,
                    dep_context):

                self.log.debug(
                    "%s dependency '%s' PASSED: %s, %s",
                    self, dep_status.dep_name, dep_status.passed, dep_status.reason
                )

                if not dep_status.passed:
                    yield dep_status

    def __repr__(self):
        return (
            "<TaskInstance: {ti.dag_id}.{ti.task_id} "
            "{ti.execution_date} [{ti.state}]>"
        ).format(ti=self)

    def next_retry_datetime(self):
        """
        Get datetime of the next retry if the task instance fails. For exponential
        backoff, retry_delay is used as base and will be converted to seconds.
        """
        delay = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            # If the min_backoff calculation is below 1, it will be converted to 0 via int. Thus,
            # we must round up prior to converting to an int, otherwise a divide by zero error
            # will occurr in the modded_hash calculation.
            min_backoff = int(math.ceil(delay.total_seconds() * (2 ** (self.try_number - 2))))
            # deterministic per task instance
            hash = int(hashlib.sha1("{}#{}#{}#{}".format(self.dag_id,
                                                         self.task_id,
                                                         self.execution_date,
                                                         self.try_number)
                                    .encode('utf-8')).hexdigest(), 16)
            # between 1 and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail.
            delay_backoff_in_seconds = min(
                modded_hash,
                timedelta.max.total_seconds() - 1
            )
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        return self.end_date + delay

    def ready_for_retry(self):
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return (self.state == State.UP_FOR_RETRY and
                self.next_retry_datetime() < timezone.utcnow())

    @provide_session
    def get_dagrun(self, session=None):
        """
        Returns the DagRun for this TaskInstance

        :param session:
        :return: DagRun
        """
        from airflow.models.dagrun import DagRun  # Avoid circular import
        dr = session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == self.execution_date
        ).first()

        return dr

    @provide_session
    def check_and_change_state_before_execution(
            self,
            verbose: bool = True,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            mark_success: bool = False,
            test_mode: bool = False,
            job_id: Optional[str] = None,
            pool: Optional[str] = None,
            session=None) -> bool:
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :type verbose: bool
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :type ignore_all_deps: bool
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :type ignore_depends_on_past: bool
        :param ignore_task_deps: Don't check the dependencies of this TaskInstance's task
        :type ignore_task_deps: bool
        :param ignore_ti_state: Disregards previous task instance state
        :type ignore_ti_state: bool
        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task
        self.refresh_from_task(task, pool_override=pool)
        self.test_mode = test_mode
        self.refresh_from_db(session=session, lock_for_update=True)
        self.job_id = job_id
        self.hostname = get_hostname()

        if not ignore_all_deps and not ignore_ti_state and self.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        # TODO: Logging needs cleanup, not clear what is being printed
        hr = "\n" + ("-" * 80)  # Line break

        if not mark_success:
            # Firstly find non-runnable and non-requeueable tis.
            # Since mark_success is not set, we do nothing.
            non_requeueable_dep_context = DepContext(
                deps=RUNNING_DEPS - REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_ti_state=ignore_ti_state,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps)
            if not self.are_dependencies_met(
                    dep_context=non_requeueable_dep_context,
                    session=session,
                    verbose=True):
                session.commit()
                return False

            # For reporting purposes, we report based on 1-indexed,
            # not 0-indexed lists (i.e. Attempt 1 instead of
            # Attempt 0 for the first attempt).
            # Set the task start date. In case it was re-scheduled use the initial
            # start date that is recorded in task_reschedule table
            self.start_date = timezone.utcnow()
            task_reschedules = TaskReschedule.find_for_task_instance(self, session)
            if task_reschedules:
                self.start_date = task_reschedules[0].start_date

            # Secondly we find non-runnable but requeueable tis. We reset its state.
            # This is because we might have hit concurrency limits,
            # e.g. because of backfilling.
            dep_context = DepContext(
                deps=REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps,
                ignore_ti_state=ignore_ti_state)
            if not self.are_dependencies_met(
                    dep_context=dep_context,
                    session=session,
                    verbose=True):
                self.state = State.NONE
                self.log.warning(hr)
                self.log.warning(
                    "Rescheduling due to concurrency limits reached "
                    "at task runtime. Attempt %s of "
                    "%s. State set to NONE.", self.try_number, self.max_tries + 1
                )
                self.log.warning(hr)
                self.queued_dttm = timezone.utcnow()
                session.merge(self)
                session.commit()
                return False

        # print status message
        self.log.info(hr)
        self.log.info("Starting attempt %s of %s", self.try_number, self.max_tries + 1)
        self.log.info(hr)
        self._try_number += 1

        if not test_mode:
            session.add(Log(State.RUNNING, self))
        self.state = State.RUNNING
        self.pid = os.getpid()
        self.end_date = None
        if not test_mode:
            session.merge(self)
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()  # type: ignore
        if verbose:
            if mark_success:
                self.log.info("Marking success for %s on %s", self.task, self.execution_date)
            else:
                self.log.info("Executing %s on %s", self.task, self.execution_date)
        return True

    @provide_session
    @Sentry.enrich_errors
    def _run_raw_task(
            self,
            mark_success: bool = False,
            test_mode: bool = False,
            job_id: Optional[str] = None,
            pool: Optional[str] = None,
            session=None) -> None:
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        """
        from airflow.sensors.base_sensor_operator import BaseSensorOperator
        from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF

        task = self.task
        self.test_mode = test_mode
        self.refresh_from_task(task, pool_override=pool)
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = get_hostname()

        context = {}  # type: Dict
        actual_start_date = timezone.utcnow()
        try:
            if not mark_success:
                context = self.get_template_context()

                task_copy = copy.copy(task)

                # Sensors in `poke` mode can block execution of DAGs when running
                # with single process executor, thus we change the mode to`reschedule`
                # to allow parallel task being scheduled and executed
                if isinstance(task_copy, BaseSensorOperator) and \
                        conf.get('core', 'executor') == "DebugExecutor":
                    self.log.warning("DebugExecutor changes sensor mode to 'reschedule'.")
                    task_copy.mode = 'reschedule'

                self.task = task_copy

                def signal_handler(signum, frame):
                    self.log.error("Received SIGTERM. Terminating subprocesses.")
                    task_copy.on_kill()
                    raise AirflowException("Task received SIGTERM signal")
                signal.signal(signal.SIGTERM, signal_handler)

                # Don't clear Xcom until the task is certain to execute
                self.clear_xcom_data()

                start_time = time.time()

                self.render_templates(context=context)
                if STORE_SERIALIZED_DAGS:
                    RTIF.write(RTIF(ti=self, render_templates=False), session=session)
                    RTIF.delete_old_records(self.task_id, self.dag_id, session=session)

                # Export context to make it available for operators to use.
                airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
                self.log.info("Exporting the following env vars:\n%s",
                              '\n'.join(["{}={}".format(k, v)
                                         for k, v in airflow_context_vars.items()]))
                os.environ.update(airflow_context_vars)
                task_copy.pre_execute(context=context)

                try:
                    if task.on_execute_callback:
                        task.on_execute_callback(context)
                except Exception as e3:
                    self.log.error("Failed when executing execute callback")
                    self.log.exception(e3)

                # If a timeout is specified for the task, make it fail
                # if it goes beyond
                result = None
                if task_copy.execution_timeout:
                    try:
                        with timeout(int(
                                task_copy.execution_timeout.total_seconds())):
                            result = task_copy.execute(context=context)
                    except AirflowTaskTimeout:
                        task_copy.on_kill()
                        raise
                else:
                    result = task_copy.execute(context=context)

                # If the task returns a result, push an XCom containing it
                if task_copy.do_xcom_push and result is not None:
                    self.xcom_push(key=XCOM_RETURN_KEY, value=result)

                task_copy.post_execute(context=context, result=result)

                end_time = time.time()
                duration = end_time - start_time
                Stats.timing(
                    'dag.{dag_id}.{task_id}.duration'.format(
                        dag_id=task_copy.dag_id,
                        task_id=task_copy.task_id),
                    duration)

                Stats.incr('operator_successes_{}'.format(
                    self.task.__class__.__name__), 1, 1)
                Stats.incr('ti_successes')
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSkipException as e:
            # Recording SKIP
            # log only if exception has any arguments to prevent log flooding
            if e.args:
                self.log.info(e)
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
            self.log.info(
                'Marking task as SKIPPED.'
                'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
                self.dag_id,
                self.task_id,
                self.execution_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                    self,
                    'execution_date') and self.execution_date else '',
                self.start_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                    self,
                    'start_date') and self.start_date else '',
                self.end_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                    self,
                    'end_date') and self.end_date else '')
        except AirflowRescheduleException as reschedule_exception:
            self.refresh_from_db()
            self._handle_reschedule(actual_start_date, reschedule_exception, test_mode, context)
            return
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success/failed externally
            # current behavior doesn't hit the success callback
            if self.state in {State.SUCCESS, State.FAILED}:
                return
            else:
                self.handle_failure(e, test_mode, context)
                raise
        except (Exception, KeyboardInterrupt) as e:
            self.handle_failure(e, test_mode, context)
            raise

        # Success callback
        try:
            if task.on_success_callback:
                task.on_success_callback(context)
        except Exception as e3:
            self.log.error("Failed when executing success callback")
            self.log.exception(e3)

        # Recording SUCCESS
        self.end_date = timezone.utcnow()
        self.log.info(
            'Marking task as SUCCESS.'
            'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
            self.dag_id,
            self.task_id,
            self.execution_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                self,
                'execution_date') and self.execution_date else '',
            self.start_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                self,
                'start_date') and self.start_date else '',
            self.end_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                self,
                'end_date') and self.end_date else '')
        self.set_duration()
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)
        session.commit()

    @provide_session
    def run(
            self,
            verbose: bool = True,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            mark_success: bool = False,
            test_mode: bool = False,
            job_id: Optional[str] = None,
            pool: Optional[str] = None,
            session=None) -> None:
        res = self.check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            session=session)
        if res:
            self._run_raw_task(
                mark_success=mark_success,
                test_mode=test_mode,
                job_id=job_id,
                pool=pool,
                session=session)

    def dry_run(self):
        task = self.task
        task_copy = copy.copy(task)
        self.task = task_copy

        self.render_templates()
        task_copy.dry_run()

    @provide_session
    def _handle_reschedule(self, actual_start_date, reschedule_exception, test_mode=False, context=None,
                           session=None):
        # Don't record reschedule request in test mode
        if test_mode:
            return

        self.end_date = timezone.utcnow()
        self.set_duration()

        # Log reschedule request
        session.add(TaskReschedule(self.task, self.execution_date, self._try_number,
                    actual_start_date, self.end_date,
                    reschedule_exception.reschedule_date))

        # set state
        self.state = State.UP_FOR_RESCHEDULE

        # Decrement try_number so subsequent runs will use the same try number and write
        # to same log file.
        self._try_number -= 1

        session.merge(self)
        session.commit()
        self.log.info('Rescheduling task, marking task as UP_FOR_RESCHEDULE')

    @provide_session
    def handle_failure(self, error, test_mode=None, context=None, session=None):
        if test_mode is None:
            test_mode = self.test_mode
        if context is None:
            context = self.get_template_context()

        self.log.exception(error)
        task = self.task
        self.end_date = timezone.utcnow()
        self.set_duration()
        Stats.incr('operator_failures_{}'.format(task.__class__.__name__), 1, 1)
        Stats.incr('ti_failures')
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Log failure duration
        session.add(TaskFail(task, self.execution_date, self.start_date, self.end_date))

        if context is not None:
            context['exception'] = error

        # Let's go deeper
        try:
            # Since this function is called only when the TaskInstance state is running,
            # try_number contains the current try_number (not the next). We
            # only mark task instance as FAILED if the next task instance
            # try_number exceeds the max_tries.
            if self.is_eligible_to_retry():
                self.state = State.UP_FOR_RETRY
                self.log.info('Marking task as UP_FOR_RETRY')
                if task.email_on_retry and task.email:
                    self.email_alert(error)
            else:
                self.state = State.FAILED
                if task.retries:
                    self.log.info(
                        'All retries failed; marking task as FAILED.'
                        'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
                        self.dag_id,
                        self.task_id,
                        self.execution_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'execution_date') and self.execution_date else '',
                        self.start_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'start_date') and self.start_date else '',
                        self.end_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'end_date') and self.end_date else '')
                else:
                    self.log.info(
                        'Marking task as FAILED.'
                        'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
                        self.dag_id,
                        self.task_id,
                        self.execution_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'execution_date') and self.execution_date else '',
                        self.start_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'start_date') and self.start_date else '',
                        self.end_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'end_date') and self.end_date else '')
                if task.email_on_failure and task.email:
                    self.email_alert(error)
        except Exception as e2:
            self.log.error('Failed to send email to: %s', task.email)
            self.log.exception(e2)

        # Handling callbacks pessimistically
        try:
            if self.state == State.UP_FOR_RETRY and task.on_retry_callback:
                task.on_retry_callback(context)
            if self.state == State.FAILED and task.on_failure_callback:
                task.on_failure_callback(context)
        except Exception as e3:
            self.log.error("Failed at executing callback")
            self.log.exception(e3)

        if not test_mode:
            session.merge(self)
        session.commit()

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry"""
        return self.task.retries and self.try_number <= self.max_tries

    @provide_session
    def get_template_context(self, session=None) -> Dict[str, Any]:
        task = self.task
        from airflow import macros

        params = {}  # type: Dict[str, Any]
        run_id = ''
        dag_run = None
        if hasattr(task, 'dag'):
            if task.dag.params:
                params.update(task.dag.params)
            from airflow.models.dagrun import DagRun  # Avoid circular import
            dag_run = (
                session.query(DagRun)
                .filter_by(
                    dag_id=task.dag.dag_id,
                    execution_date=self.execution_date)
                .first()
            )
            run_id = dag_run.run_id if dag_run else None
            session.expunge_all()
            session.commit()

        ds = self.execution_date.strftime('%Y-%m-%d')
        ts = self.execution_date.isoformat()
        yesterday_ds = (self.execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (self.execution_date + timedelta(1)).strftime('%Y-%m-%d')

        # For manually triggered dagruns that aren't run on a schedule, next/previous
        # schedule dates don't make sense, and should be set to execution date for
        # consistency with how execution_date is set for manually triggered tasks, i.e.
        # triggered_date == execution_date.
        if dag_run and dag_run.external_trigger:
            prev_execution_date = self.execution_date
            next_execution_date = self.execution_date
        else:
            prev_execution_date = task.dag.previous_schedule(self.execution_date)
            next_execution_date = task.dag.following_schedule(self.execution_date)

        next_ds = None
        next_ds_nodash = None
        if next_execution_date:
            next_ds = next_execution_date.strftime('%Y-%m-%d')
            next_ds_nodash = next_ds.replace('-', '')
            next_execution_date = pendulum.instance(next_execution_date)

        prev_ds = None
        prev_ds_nodash = None
        if prev_execution_date:
            prev_ds = prev_execution_date.strftime('%Y-%m-%d')
            prev_ds_nodash = prev_ds.replace('-', '')
            prev_execution_date = pendulum.instance(prev_execution_date)

        ds_nodash = ds.replace('-', '')
        ts_nodash = self.execution_date.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz = ts.replace('-', '').replace(':', '')
        yesterday_ds_nodash = yesterday_ds.replace('-', '')
        tomorrow_ds_nodash = tomorrow_ds.replace('-', '')

        ti_key_str = "{dag_id}__{task_id}__{ds_nodash}".format(
            dag_id=task.dag_id, task_id=task.task_id, ds_nodash=ds_nodash)

        if task.params:
            params.update(task.params)

        if conf.getboolean('core', 'dag_run_conf_overrides_params'):
            self.overwrite_params_with_dag_run_conf(params=params, dag_run=dag_run)

        class VariableAccessor:
            """
            Wrapper around Variable. This way you can get variables in
            templates by using ``{{ var.value.variable_name }}`` or
            ``{{ var.value.get('variable_name', 'fallback') }}``.
            """
            def __init__(self):
                self.var = None

            def __getattr__(
                self,
                item: str,
            ):
                self.var = Variable.get(item)
                return self.var

            def __repr__(self):
                return str(self.var)

            @staticmethod
            def get(
                item: str,
                default_var: Any = Variable._Variable__NO_DEFAULT_SENTINEL,
            ):
                return Variable.get(item, default_var=default_var)

        class VariableJsonAccessor:
            """
            Wrapper around Variable. This way you can get variables in
            templates by using ``{{ var.json.variable_name }}`` or
            ``{{ var.json.get('variable_name', {'fall': 'back'}) }}``.
            """
            def __init__(self):
                self.var = None

            def __getattr__(
                self,
                item: str,
            ):
                self.var = Variable.get(item, deserialize_json=True)
                return self.var

            def __repr__(self):
                return str(self.var)

            @staticmethod
            def get(
                item: str,
                default_var: Any = Variable._Variable__NO_DEFAULT_SENTINEL,
            ):
                return Variable.get(item, default_var=default_var, deserialize_json=True)

        return {
            'conf': conf,
            'dag': task.dag,
            'dag_run': dag_run,
            'ds': ds,
            'ds_nodash': ds_nodash,
            'execution_date': pendulum.instance(self.execution_date),
            'inlets': task.inlets,
            'macros': macros,
            'next_ds': next_ds,
            'next_ds_nodash': next_ds_nodash,
            'next_execution_date': next_execution_date,
            'outlets': task.outlets,
            'params': params,
            'prev_ds': prev_ds,
            'prev_ds_nodash': prev_ds_nodash,
            'prev_execution_date': prev_execution_date,
            'prev_execution_date_success': lazy_object_proxy.Proxy(
                lambda: self.get_previous_execution_date(state=State.SUCCESS)),
            'prev_start_date_success': lazy_object_proxy.Proxy(
                lambda: self.get_previous_start_date(state=State.SUCCESS)),
            'run_id': run_id,
            'task': task,
            'task_instance': self,
            'task_instance_key_str': ti_key_str,
            'test_mode': self.test_mode,
            'ti': self,
            'tomorrow_ds': tomorrow_ds,
            'tomorrow_ds_nodash': tomorrow_ds_nodash,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'ts_nodash_with_tz': ts_nodash_with_tz,
            'var': {
                'json': VariableJsonAccessor(),
                'value': VariableAccessor(),
            },
            'yesterday_ds': yesterday_ds,
            'yesterday_ds_nodash': yesterday_ds_nodash,
        }

    def get_rendered_template_fields(self):
        """
        Fetch rendered template fields from DB if Serialization is enabled.
        Else just render the templates
        """
        from airflow.models.renderedtifields import RenderedTaskInstanceFields
        if STORE_SERIALIZED_DAGS:
            rtif = RenderedTaskInstanceFields.get_templated_fields(self)
            if rtif:
                for field_name, rendered_value in rtif.items():
                    setattr(self.task, field_name, rendered_value)
            else:
                try:
                    self.render_templates()
                except (TemplateAssertionError, UndefinedError) as e:
                    raise AirflowException(
                        "Webserver does not have access to User-defined Macros or Filters "
                        "when Dag Serialization is enabled. Hence for the task that have not yet "
                        "started running, please use 'airflow tasks render' for debugging the "
                        "rendering of template_fields."
                    ) from e
        else:
            self.render_templates()

    def overwrite_params_with_dag_run_conf(self, params, dag_run):
        if dag_run and dag_run.conf:
            params.update(dag_run.conf)

    def render_templates(self, context: Optional[Dict] = None) -> None:
        """Render templates in the operator fields."""
        if not context:
            context = self.get_template_context()

        self.task.render_template_fields(context)

    def email_alert(self, exception):
        exception_html = str(exception).replace('\n', '<br>')
        jinja_context = self.get_template_context()
        # This function is called after changing the state
        # from State.RUNNING so use prev_attempted_tries.
        jinja_context.update(dict(
            exception=exception,
            exception_html=exception_html,
            try_number=self.prev_attempted_tries,
            max_tries=self.max_tries))

        jinja_env = self.task.get_template_env()

        default_subject = 'Airflow alert: {{ti}}'
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        default_html_content = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>{{exception_html}}<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Log file: {{ti.log_filepath}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        def render(key, content):
            if conf.has_option('email', key):
                path = conf.get('email', key)
                with open(path) as file:
                    content = file.read()

            return jinja_env.from_string(content).render(**jinja_context)

        subject = render('subject_template', default_subject)
        html_content = render('html_content_template', default_html_content)
        try:
            send_email(self.task.email, subject, html_content)
        except Exception:
            default_html_content_err = (
                'Try {{try_number}} out of {{max_tries + 1}}<br>'
                'Exception:<br>Failed attempt to attach error logs<br>'
                'Log: <a href="{{ti.log_url}}">Link</a><br>'
                'Host: {{ti.hostname}}<br>'
                'Log file: {{ti.log_filepath}}<br>'
                'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
            )
            html_content_err = render('html_content_template', default_html_content_err)
            send_email(self.task.email, subject, html_content_err)

    def set_duration(self) -> None:
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None

    def xcom_push(
            self,
            key: str,
            value: Any,
            execution_date: Optional[datetime] = None) -> None:
        """
        Make an XCom available for tasks to pull.

        :param key: A key for the XCom
        :type key: str
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :type value: any pickleable object
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        :type execution_date: datetime
        """

        if execution_date and execution_date < self.execution_date:
            raise ValueError(
                'execution_date can not be in the past (current '
                'execution_date is {}; received {})'.format(
                    self.execution_date, execution_date))

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            execution_date=execution_date or self.execution_date)

    def xcom_pull(
            self,
            task_ids: Optional[Union[str, Iterable[str]]] = None,
            dag_id: Optional[str] = None,
            key: str = XCOM_RETURN_KEY,
            include_prior_dates: bool = False) -> Any:
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :type key: str
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: str or iterable of strings (representing task_ids)
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: str
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        """

        if dag_id is None:
            dag_id = self.dag_id

        query = XCom.get_many(
            execution_date=self.execution_date,
            key=key,
            dag_ids=dag_id,
            task_ids=task_ids,
            include_prior_dates=include_prior_dates
        ).with_entities(XCom.value)

        # Since we're only fetching the values field, and not the
        # whole class, the @recreate annotation does not kick in.
        # Therefore we need to deserialize the fields by ourselves.

        if is_container(task_ids):
            return [XCom.deserialize_value(xcom) for xcom in query]
        else:
            xcom = query.first()
            if xcom:
                return XCom.deserialize_value(xcom)

    @provide_session
    def get_num_running_task_instances(self, session):
        # .count() is inefficient
        return session.query(func.count()).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.state == State.RUNNING
        ).scalar()

    def init_run_context(self, raw=False):
        """
        Sets the log context.
        """
        self.raw = raw
        self._set_context(self)

    @staticmethod
    def filter_for_tis(
        tis: Iterable[Union["TaskInstance", TaskInstanceKeyType]]
    ) -> Optional[BooleanClauseList]:
        """Returns SQLAlchemy filter to query selected task instances"""
        TI = TaskInstance
        if not tis:
            return None
        if all(isinstance(t, tuple) for t in tis):
            filter_for_tis = ([and_(TI.dag_id == dag_id,
                                    TI.task_id == task_id,
                                    TI.execution_date == execution_date)
                               for dag_id, task_id, execution_date, _ in tis])
            return or_(*filter_for_tis)
        if all(isinstance(t, TaskInstance) for t in tis):
            filter_for_tis = ([and_(TI.dag_id == ti.dag_id,  # type: ignore
                                    TI.task_id == ti.task_id,  # type: ignore
                                    TI.execution_date == ti.execution_date)  # type: ignore
                               for ti in tis])
            return or_(*filter_for_tis)

        raise TypeError("All elements must have the same type: `TaskInstance` or `TaskInstanceKey`.")


# State of the task instance.
# Stores string version of the task state.
TaskInstanceStateType = Tuple[TaskInstanceKeyType, str]


class SimpleTaskInstance:
    """
    Simplified Task Instance.

    Used to send data between processes via Queues.
    """
    def __init__(self, ti: TaskInstance):
        self._dag_id: str = ti.dag_id
        self._task_id: str = ti.task_id
        self._execution_date: datetime = ti.execution_date
        self._start_date: datetime = ti.start_date
        self._end_date: datetime = ti.end_date
        self._try_number: int = ti.try_number
        self._state: str = ti.state
        self._executor_config: Any = ti.executor_config
        self._run_as_user: Optional[str] = None
        if hasattr(ti, 'run_as_user'):
            self._run_as_user = ti.run_as_user
        self._pool: Optional[str] = None
        if hasattr(ti, 'pool'):
            self._pool = ti.pool
        self._priority_weight: Optional[int] = None
        if hasattr(ti, 'priority_weight'):
            self._priority_weight = ti.priority_weight
        self._queue: str = ti.queue
        self._key = ti.key

    # pylint: disable=missing-docstring
    @property
    def dag_id(self) -> str:
        return self._dag_id

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def execution_date(self) -> datetime:
        return self._execution_date

    @property
    def start_date(self) -> datetime:
        return self._start_date

    @property
    def end_date(self) -> datetime:
        return self._end_date

    @property
    def try_number(self) -> int:
        return self._try_number

    @property
    def state(self) -> str:
        return self._state

    @property
    def pool(self) -> Any:
        return self._pool

    @property
    def priority_weight(self) -> Optional[int]:
        return self._priority_weight

    @property
    def queue(self) -> str:
        return self._queue

    @property
    def key(self) -> TaskInstanceKeyType:
        return self._key

    @property
    def executor_config(self):
        return self._executor_config

    @provide_session
    def construct_task_instance(self, session=None, lock_for_update=False) -> TaskInstance:
        """
        Construct a TaskInstance from the database based on the primary key

        :param session: DB session.
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        :return: the task instance constructed
        """

        qry = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self._dag_id,
            TaskInstance.task_id == self._task_id,
            TaskInstance.execution_date == self._execution_date)

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        return ti
```

### Task状态显示接口

可以在airflow\task中编写Tag 状态显示api视图接口,包括所需的后端逻辑方法

如下面逻辑接口代码：

get_task_instance.py

```python
"""Task Instance APIs."""
from datetime import datetime

from airflow.api.common.experimental import check_and_get_dag, check_and_get_dagrun
from airflow.exceptions import TaskInstanceNotFound
from airflow.models import TaskInstance


def get_task_instance(dag_id: str, task_id: str, execution_date: datetime) -> TaskInstance:
    """Return the task object identified by the given dag_id and task_id."""
    dag = check_and_get_dag(dag_id, task_id)

    dagrun = check_and_get_dagrun(dag=dag, execution_date=execution_date)
    # Get task instance object and check that it exists
    task_instance = dagrun.get_task_instance(task_id)
    if not task_instance:
        error_message = ('Task {} instance for date {} not found'
                         .format(task_id, execution_date))
        raise TaskInstanceNotFound(error_message)

    return task_instance
```