/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.ExecutionStats.ExecutionStatsSnapshot;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.facebook.presto.execution.SharedBufferInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.OperatorStats.SplitExecutionStats;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class HttpRemoteTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTask.class);

    private final Session session;
    private final String nodeId;
    private final PlanFragment planFragment;
    private final int maxConsecutiveErrorCount;
    private final Duration minErrorDuration;

    private final AtomicLong nextSplitId = new AtomicLong();

    // a lock on "this" is required for mutual exclusion
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();
    @GuardedBy("this")
    private CurrentRequest currentRequest;
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
    @GuardedBy("this")
    private boolean noMoreSplits;
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, URI> exchangeLocations = HashMultimap.create();
    @GuardedBy("this")
    private boolean noMoreExchangeLocations;
    @GuardedBy("this")
    private final Set<String> outputIds = new TreeSet<>();
    @GuardedBy("this")
    private boolean noMoreOutputIds;
    @GuardedBy("this")
    private final List<TaskStateChangeListener> stateChangeListeners = new ArrayList<>();

    @GuardedBy("this")
    private UpdateLoop updateLoop;

    private final AsyncHttpClient httpClient;
    private final Executor executor;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final List<TupleInfo> tupleInfos;
    private final Map<PlanNodeId, OutputReceiver> outputReceivers;

    private final RateLimiter errorRequestRateLimiter = RateLimiter.create(0.1);

    private final AtomicLong lastSuccessfulRequest = new AtomicLong(System.nanoTime());
    private final AtomicLong errorCount = new AtomicLong();
    private final Queue<Throwable> errorsSinceLastSuccess = new ConcurrentLinkedQueue<>();

    public HttpRemoteTask(Session session,
            TaskId taskId,
            Node node,
            URI location,
            PlanFragment planFragment,
            Split initialSplit,
            Map<PlanNodeId, OutputReceiver> outputReceivers,
            Multimap<PlanNodeId, URI> initialExchangeLocations,
            Set<String> initialOutputIds,
            AsyncHttpClient httpClient,
            Executor executor,
            int maxConsecutiveErrorCount,
            Duration minErrorDuration,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(planFragment, "planFragment1 is null");
        Preconditions.checkNotNull(outputReceivers, "outputReceivers is null");
        Preconditions.checkNotNull(initialOutputIds, "initialOutputIds is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(executor, "executor is null");
        Preconditions.checkNotNull(taskInfoCodec, "taskInfoCodec is null");
        Preconditions.checkNotNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");

        this.session = session;
        this.nodeId = node.getNodeIdentifier();
        this.planFragment = planFragment;
        this.outputReceivers = ImmutableMap.copyOf(outputReceivers);
        this.outputIds.addAll(initialOutputIds);
        this.httpClient = httpClient;
        this.executor = executor;
        this.taskInfoCodec = taskInfoCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.tupleInfos = planFragment.getTupleInfos();
        this.maxConsecutiveErrorCount = maxConsecutiveErrorCount;
        this.minErrorDuration = minErrorDuration;


        for (Entry<PlanNodeId, URI> entry : initialExchangeLocations.entries()) {
            ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), createRemoteSplitFor(entry.getValue()));
            pendingSplits.put(entry.getKey(), scheduledSplit);
        }

        this.exchangeLocations.putAll(initialExchangeLocations);

        List<BufferInfo> bufferStates = ImmutableList.copyOf(transform(initialOutputIds, new Function<String, BufferInfo>()
        {
            @Override
            public BufferInfo apply(String outputId)
            {
                return new BufferInfo(outputId, false, 0, 0);
            }
        }));

        if (initialSplit != null) {
            checkState(planFragment.isPartitioned(), "Plan is not partitioned");
            pendingSplits.put(planFragment.getPartitionedSource(), new ScheduledSplit(nextSplitId.getAndIncrement(), initialSplit));
        }

        taskInfo.set(new TaskInfo(
                taskId,
                TaskInfo.MIN_VERSION,
                TaskState.PLANNED,
                location,
                new SharedBufferInfo(QueueState.OPEN, 0, bufferStates),
                ImmutableSet.<PlanNodeId>of(),
                new ExecutionStatsSnapshot(),
                ImmutableList.<SplitExecutionStats>of(),
                ImmutableList.<FailureInfo>of(),
                ImmutableMap.<PlanNodeId, Set<?>>of()));
    }

    @Override
    public TaskId getTaskId()
    {
        return taskInfo.get().getTaskId();
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskInfo.get();
    }

    @Override
    public void addSplit(Split split)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Update state can not be called while holding a lock on this");

        synchronized (this) {
            Preconditions.checkNotNull(split, "split is null");
            Preconditions.checkState(!noMoreSplits, "noMoreSplits has already been set");
            Preconditions.checkState(planFragment.isPartitioned(), "Plan is not partitioned");

            // only add pending split if not done
            if (!getTaskInfo().getState().isDone()) {
                pendingSplits.put(planFragment.getPartitionedSource(), new ScheduledSplit(nextSplitId.getAndIncrement(), split));
            }
        }
        updateState(false);
    }

    @Override
    public void noMoreSplits()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Update state can not be called while holding a lock on this");

        synchronized (this) {
            Preconditions.checkState(!noMoreSplits, "noMoreSplits has already been set");
            noMoreSplits = true;
        }
        updateState(false);
    }

    @Override
    public void addExchangeLocations(Multimap<PlanNodeId, URI> additionalLocations, boolean noMore)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Update state can not be called while holding a lock on this");

        // determine which locations are new
        synchronized (this) {
            Preconditions.checkState(!noMoreExchangeLocations || exchangeLocations.entries().containsAll(additionalLocations.entries()),
                    "Locations can not be added after noMoreExchangeLocations has been set");

            SetMultimap<PlanNodeId, URI> newExchangeLocations = HashMultimap.create(additionalLocations);
            newExchangeLocations.entries().removeAll(exchangeLocations.entries());

            // only add pending split if not done
            if (!getTaskInfo().getState().isDone()) {
                for (Entry<PlanNodeId, URI> entry : newExchangeLocations.entries()) {
                    ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), createRemoteSplitFor(entry.getValue()));
                    pendingSplits.put(entry.getKey(), scheduledSplit);
                }
            }
            noMoreExchangeLocations = noMore;

            this.exchangeLocations.putAll(additionalLocations);
        }
        updateState(false);
    }

    @Override
    public void addOutputBuffers(Set<String> outputBuffers, boolean noMore)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Update state can not be called while holding a lock on this");
        synchronized (this) {
            if (noMoreOutputIds == noMore && this.outputIds.containsAll(outputBuffers)) {
                // duplicate request
                return;
            }
            Preconditions.checkState(!noMoreOutputIds, "New buffers can not be added after noMoreOutputIds has been set");
            noMoreOutputIds = noMore;
            this.outputIds.addAll(outputBuffers);
        }
        updateState(false);
    }

    @Override
    public synchronized void addStateChangeListener(TaskStateChangeListener stateChangeListener)
    {
        stateChangeListeners.add(stateChangeListener);
    }

    private void fireStateChangedEvent(final TaskInfo newValue)
    {
        final ImmutableList<TaskStateChangeListener> stateChangeListeners;
        synchronized (this) {
            stateChangeListeners = ImmutableList.copyOf(this.stateChangeListeners);
        }
        executor.execute(new Runnable() {
            @Override
            public void run()
            {
                for (TaskStateChangeListener stateChangeListener : stateChangeListeners) {
                    try {
                        stateChangeListener.stateChanged(newValue);
                    }
                    catch (Exception e) {
                        log.error(e, "Error notifying state change listener for task %s", newValue.getTaskId());
                    }
                }
            }
        });
    }

    private synchronized void updateTaskInfo(TaskInfo newValue)
    {
        TaskInfo oldValue = taskInfo.get();
        if (oldValue.getState().isDone()) {
            // never update if the task has reached a terminal state
            return;
        }
        if (newValue.getVersion() < oldValue.getVersion()) {
            // don't update to an older version (same version is ok)
            return;
        }

        boolean stateChanged = newValue.getState() != oldValue.getState();

        taskInfo.set(newValue);

        for (Map.Entry<PlanNodeId, Set<?>> entry : newValue.getOutputs().entrySet()) {
            OutputReceiver outputReceiver =  outputReceivers.get(entry.getKey());
            checkState(outputReceiver != null, "Got Result for node %s which is not an output receiver!", entry.getKey());
            for (Object result : entry.getValue()) {
                outputReceiver.updateOutput(result);
            }
        }

        if (newValue.getState().isDone()) {
            // splits can be huge so clear the list
            pendingSplits.clear();
        }

        if (stateChanged) {
            fireStateChangedEvent(newValue);
        }
    }

    @Override
    public ListenableFuture<?> updateState(boolean forceRefresh)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Update state can not be called while holding a lock on this");

        // if we have an old request outstanding, cancel it
        cancelStaleRequest();

        Request request;
        CurrentRequest currentRequest;

        synchronized (this) {
            // don't update if the task hasn't been started yet or if it is already finished
            if (taskInfo.get().getState().isDone()) {
                return Futures.immediateFuture(null);
            }

            if (updateLoop == null) {
                updateLoop = new UpdateLoop();
                updateLoop.start();
            }

            if (!forceRefresh) {
                if (this.currentRequest != null) {
                    if (!this.currentRequest.isDone()) {
                        // request is still running, but when it finishes, it should update again
                        this.currentRequest.requestAnotherUpdate();

                        // todo return existing pending request future?
                        return Futures.immediateFuture(null);
                    }

                    this.currentRequest = null;
                }

                // don't update too fast in the face of errors
                if (errorCount.get() > 0) {
                    errorRequestRateLimiter.acquire();
                }
            }

            List<TaskSource> sources = getSources();
            currentRequest = new CurrentRequest(sources);
            TaskUpdateRequest updateRequest = new TaskUpdateRequest(session,
                    planFragment,
                    sources,
                    new OutputBuffers(outputIds, noMoreOutputIds));

            request = preparePost()
                    .setUri(uriBuilderFrom(taskInfo.get().getSelf()).build())
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                    .setBodyGenerator(jsonBodyGenerator(taskUpdateRequestCodec, updateRequest))
                    .build();

            this.currentRequest = currentRequest;
        }

        ListenableFuture<JsonResponse<TaskInfo>> future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        currentRequest.setRequestFuture(future);
        return currentRequest;
    }

    private void cancelStaleRequest()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Update state can not be called while holding a lock on this");

        CurrentRequest requestToCancel;
        synchronized (this) {
            if (currentRequest == null) {
                return;
            }
            if (Duration.nanosSince(currentRequest.startTime).compareTo(new Duration(2, TimeUnit.SECONDS)) < 0) {
                return;
            }

            requestToCancel = currentRequest;
            currentRequest = null;
        }
        requestToCancel.cancel(true);
    }

    @Override
    public synchronized int getQueuedSplits()
    {
        int pendingSplitCount = 0;
        if (planFragment.isPartitioned()) {
            pendingSplitCount = pendingSplits.get(planFragment.getPartitionedSource()).size();
        }
        return pendingSplitCount + taskInfo.get().getStats().getQueuedSplits();
    }

    private synchronized List<TaskSource> getSources()
    {
        ImmutableList.Builder<TaskSource> sources = ImmutableList.builder();
        if (planFragment.isPartitioned()) {
            Set<ScheduledSplit> splits = pendingSplits.get(planFragment.getPartitionedSource());
            if (!splits.isEmpty() || noMoreSplits) {
                sources.add(new TaskSource(planFragment.getPartitionedSource(), splits, noMoreSplits));
            }
        }
        for (PlanNode planNode : planFragment.getSources()) {
            PlanNodeId planNodeId = planNode.getId();
            if (!planNodeId.equals(planFragment.getPartitionedSource())) {
                Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
                if (!splits.isEmpty() || noMoreExchangeLocations) {
                    sources.add(new TaskSource(planNodeId, splits, noMoreExchangeLocations));
                }
            }
        }
        return sources.build();
    }

    private synchronized void acknowledgeSources(List<TaskSource> sources)
    {
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            for (ScheduledSplit split : source.getSplits()) {
                pendingSplits.remove(planNodeId, split);
            }
        }
    }

    @Override
    public void cancel()
    {
        CurrentRequest requestToCancel;
        TaskInfo taskInfo;
        synchronized (this) {
            // clear pending splits to free memory
            pendingSplits.clear();

            // cancel pending request
            requestToCancel = currentRequest;

            // mark task as canceled (if not already done)
            taskInfo = getTaskInfo();
            updateTaskInfo(new TaskInfo(taskInfo.getTaskId(),
                    TaskInfo.MAX_VERSION,
                    TaskState.CANCELED,
                    taskInfo.getSelf(),
                    taskInfo.getOutputBuffers(),
                    taskInfo.getNoMoreSplits(),
                    taskInfo.getStats(),
                    ImmutableList.<SplitExecutionStats>of(),
                    ImmutableList.<FailureInfo>of(),
                    ImmutableMap.<PlanNodeId, Set<?>>of()));
        }

        // must cancel out side of synchronized block to avoid potential deadlocks
        if (requestToCancel != null) {
            requestToCancel.cancel(true);
        }

        // fire delete to task and ignore response
        if (taskInfo.getSelf() != null) {
            final long start = System.nanoTime();
            final Request request = prepareDelete().setUri(taskInfo.getSelf()).build();
            Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), new FutureCallback<StatusResponse>() {
                @Override
                public void onSuccess(StatusResponse result)
                {
                    // assume any response is good enough
                }

                @Override
                public void onFailure(Throwable t)
                {
                    // reschedule
                    if (Duration.nanosSince(start).compareTo(new Duration(2, TimeUnit.MINUTES)) < 0) {
                        Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), this, executor);
                    }
                    else {
                        log.error(t, "Unable to cancel task at %s", request.getUri());
                    }
                }
            }, executor);
        }
    }

    private void requestFailed(long startTime, Throwable reason)
    {
        // remember the first 10 errors
        if (errorsSinceLastSuccess.size() < 10) {
            errorsSinceLastSuccess.add(reason);
        }

        // fail the task, if we have more than X failures in a row and more than Y seconds have passed since the last request
        long errorCount = this.errorCount.incrementAndGet();
        Duration timeSinceLastSuccess = Duration.nanosSince(lastSuccessfulRequest.get());
        if (errorCount > maxConsecutiveErrorCount && timeSinceLastSuccess.compareTo(minErrorDuration) > 0) {
            // it is weird to mark the task failed locally and then cancel the remote task, but there is no way to tell a remote task that it is failed
            Duration duration = Duration.nanosSince(startTime);
            DateTime start = new DateTime(NANOSECONDS.toMillis(System.currentTimeMillis() - (long) duration.toMillis()));
            DateTime end = new DateTime();
            RuntimeException exception = new RuntimeException(String.format("Too many requests to %s failed: %s failures: %s time since last: start %s: end %s: duration %s",
                    taskInfo.get().getSelf(),
                    errorCount,
                    timeSinceLastSuccess,
                    start,
                    end,
                    duration));
            for (Throwable error : errorsSinceLastSuccess) {
                exception.addSuppressed(error);
            }
            failTask(exception);
            cancel();
        }
    }

    /**
     * Move the task directly to the failed state
     */
    private void failTask(Exception cause)
    {
        TaskInfo taskInfo = getTaskInfo();
        updateTaskInfo(new TaskInfo(taskInfo.getTaskId(),
                TaskInfo.MAX_VERSION,
                TaskState.FAILED,
                taskInfo.getSelf(),
                taskInfo.getOutputBuffers(),
                taskInfo.getNoMoreSplits(),
                taskInfo.getStats(),
                ImmutableList.<SplitExecutionStats>of(),
                ImmutableList.of(toFailure(cause)),
                taskInfo.getOutputs()));
    }

    private RemoteSplit createRemoteSplitFor(URI taskLocation)
    {
        URI splitLocation = uriBuilderFrom(taskLocation).appendPath("results").appendPath(nodeId).build();
        return new RemoteSplit(splitLocation, tupleInfos);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(getTaskInfo())
                .toString();
    }


    private class CurrentRequest
            extends AbstractFuture<Void>
            implements FutureCallback<JsonResponse<TaskInfo>>
    {
        private final long startTime = System.nanoTime();

        private final List<TaskSource> sources;

        @GuardedBy("this")
        private Future<?> requestFuture;

        @GuardedBy("this")
        private boolean anotherUpdateRequested;

        private CurrentRequest(List<TaskSource> sources)
        {
            this.sources = ImmutableList.copyOf(sources);
        }

        public void requestAnotherUpdate()
        {
            synchronized (HttpRemoteTask.this) {
                this.anotherUpdateRequested = true;
            }
        }

        public void setRequestFuture(ListenableFuture<JsonResponse<TaskInfo>> requestFuture)
        {
            synchronized (HttpRemoteTask.this) {
                Preconditions.checkNotNull(requestFuture, "requestFuture is null");
                Preconditions.checkState(this.requestFuture == null, "requestFuture already set");

                this.requestFuture = requestFuture;
            }

            if (isDone()) {
                requestFuture.cancel(true);
            } else {
                Futures.addCallback(requestFuture, this, executor);
            }
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // cancel will trigger call backs that can result in a dead lock
            Preconditions.checkState(!Thread.holdsLock(HttpRemoteTask.this), "Update state can not be called while holding a lock on HttpRemoteTask.this");

            Future<?> requestToCancel;
            synchronized (HttpRemoteTask.this) {
                requestToCancel = requestFuture;
            }

            // must cancel out side of synchronized block to avoid potential deadlocks
            if (requestToCancel != null) {
                requestFuture.cancel(true);
            }
            return super.cancel(true);
        }

        @Override
        public void onSuccess(JsonResponse<TaskInfo> response)
        {
            try {
                TaskInfo taskInfo = getTaskInfo();
                if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                    // update task info
                    updateTaskInfo(response.getValue());
                    lastSuccessfulRequest.set(System.nanoTime());
                    errorCount.set(0);
                    errorsSinceLastSuccess.clear();

                    // remove acknowledged splits, which frees memory
                    acknowledgeSources(sources);
                }
                else if (response.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE.code()) {
                    requestFailed(startTime, new RuntimeException("Server at %s returned SERVICE_UNAVAILABLE"));
                }
                else {
                    // Something is broken in the server or the client, so fail the task immediately (includes 500 errors)
                    Exception cause = response.getException();
                    if (cause == null) {
                        cause = new RuntimeException(String.format("Expected response code from %s to be %s, but was %s: %s",
                                taskInfo.getSelf(),
                                HttpStatus.OK.code(),
                                response.getStatusCode(),
                                response.getStatusMessage()));
                    }
                    log.debug(cause, "Remote task failed: %s", taskInfo.getSelf());
                    failTask(cause);
                }
            }
            catch (Throwable t) {
                // cancellation is not a failure
                if (!(t instanceof CancellationException)) {
                    setException(t);
                    requestFailed(startTime, t);
                }
            }
            finally {
                set(null);
                updateIfNecessary();
            }
        }

        private void updateIfNecessary()
        {
            synchronized (HttpRemoteTask.this) {
                if (!anotherUpdateRequested) {
                    return;
                }

                // if this is no longer the current request, don't do anything
                if (currentRequest != this) {
                    return;
                }
            }

            updateState(false);
        }

        @Override
        public void onFailure(Throwable t)
        {
            setException(t);

            if (!(t instanceof CancellationException)) {
                TaskInfo taskInfo = getTaskInfo();
                if (isSocketError(t)) {
                    log.warn("%s task %s: %s: %s", "Error updating", taskInfo.getTaskId(), t.getMessage(), taskInfo.getSelf());
                }
                else {
                    log.warn(t, "%s task %s: %s", "Error updating", taskInfo.getTaskId(), taskInfo.getSelf());
                }
            }

            requestFailed(startTime, t);
            updateState(false);
        }


        @Override
        protected boolean set(@Nullable Void value)
        {
            // these calls trigger callbacks that can cause a dead lock if a lock is held on this
            Preconditions.checkState(!Thread.holdsLock(HttpRemoteTask.this), "Update state can not be called while holding a lock on HttpRemoteTask.this");
            // this code does not synchronize on itself, but leave this here in case someone decides that is a good idea one day
            Preconditions.checkState(!Thread.holdsLock(this), "Update state can not be called while holding a lock on this");
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            // these calls trigger callbacks that can cause a dead lock if a lock is held on this
            Preconditions.checkState(!Thread.holdsLock(HttpRemoteTask.this), "Update state can not be called while holding a lock on HttpRemoteTask.this");
            // this code does not synchronize on itself, but leave this here in case someone decides that is a good idea one day
            Preconditions.checkState(!Thread.holdsLock(this), "Update state can not be called while holding a lock on this");
            return super.setException(throwable);
        }

    }

    /**
     * Continuous update loop for task info.  Wait for a short period for task state to change, and
     * if it does not, return the current state of the task.  This will cause stats to be updated at
     * a regular interval, and state changes will be immediately recorded.
     */
    private class UpdateLoop
            implements FutureCallback<JsonResponse<TaskInfo>>
    {
        private final Request request;
        private final AtomicBoolean running = new AtomicBoolean();
        // if we are getting errors, don't schedule more than 10 requests a second
        private final RateLimiter errorRequestRateLimiter = RateLimiter.create(0.1);
        private ListenableFuture<JsonResponse<TaskInfo>> future;

        private UpdateLoop()
        {
            URI build = uriBuilderFrom(taskInfo.get().getSelf())
                    .addParameter("waitForStateChange", "200ms")
                    .build();

            request = prepareGet()
                    .setUri(build)
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                    .build();
        }

        public void start()
        {
            running.set(true);
            reschedule();
        }

        public void stop()
        {
            running.set(false);
        }

        private synchronized void reschedule()
        {
            // stopped or done?
            if (!running.get() || taskInfo.get().getState().isDone()) {
                return;
            }

            // outstanding request?
            if (future != null && !future.isDone()) {
                log.error("Can not reschedule update because an update is already running");
            }

            future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
            Futures.addCallback(future, this, executor);
        }

        @Override
        public void onSuccess(JsonResponse<TaskInfo> response)
        {
            try {
                TaskInfo taskInfo = getTaskInfo();
                int statusCode = response.getStatusCode();
                if (statusCode == HttpStatus.OK.code() && response.hasValue()) {
                    // update task info
                    updateTaskInfo(response.getValue());
                    lastSuccessfulRequest.set(System.nanoTime());
                    errorCount.set(0);
                    errorsSinceLastSuccess.clear();
                }
                else if (statusCode != HttpStatus.GONE.code() && statusCode != HttpStatus.SERVICE_UNAVAILABLE.code()) {
                    // Something is broken in the server or the client, so fail the task immediately (includes 500 errors)
                    Exception cause = response.getException();
                    if (cause == null) {
                        cause = new RuntimeException(String.format("Expected response code from %s to be %s, but was %s: %s",
                                taskInfo.getSelf(),
                                HttpStatus.OK.code(),
                                statusCode,
                                response.getStatusMessage()));
                    }
                    log.debug(cause, "Remote task failed: %s", taskInfo.getSelf());
                    failTask(cause);
                }
            }
            catch (Throwable t) {
                log.error(t, "Error processing task update response: %s", taskInfo.get().getSelf());
                errorRequestRateLimiter.acquire();
            }
            finally {
                // if task has finished, the reschedule call will do nothing
                reschedule();
            }
        }

        @Override
        public void onFailure(Throwable t)
        {
            TaskInfo info = taskInfo.get();
            if (isSocketError(t)) {
                log.warn("%s task %s: %s: %s", "Error updating", info.getTaskId(), t.getMessage(), info.getSelf());
            }
            else {
                log.warn(t, "%s task %s: %s", "Error updating", info.getTaskId(), info.getSelf());
            }
            errorRequestRateLimiter.acquire();
            reschedule();
        }
    }

    private static boolean isSocketError(Throwable t)
    {
        while (t != null) {
            if ((t instanceof SocketException) || (t instanceof SocketTimeoutException)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
