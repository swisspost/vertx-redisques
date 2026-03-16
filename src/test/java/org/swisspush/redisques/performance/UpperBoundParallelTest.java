package org.swisspush.redisques.performance;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.redisques.exception.ResourceExhaustionException;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newWastefulExceptionFactory;

@RunWith(VertxUnitRunner.class)
public class UpperBoundParallelTest {

    private UpperBoundParallel target;

    private Vertx vertx;
    private Semaphore limit;

    @Before
    public void before() {
        vertx = Vertx.vertx();
        target = new UpperBoundParallel(vertx, newWastefulExceptionFactory());
        limit = new Semaphore(1);
    }

    @Test
    public void justASimpleSmokeTest(TestContext testContext) {
        Async async = testContext.async();
        final int availTokens = limit.availablePermits();
        target.request(limit, null, new UpperBoundParallel.Mentor<Void>() {
            Iterator<String> iter = List.of("input-one", "input-two", "input-three").iterator();
            @Override public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void unused) {
                if(iter.hasNext()){
                    String elem = iter.next();
                    vertx.runOnContext((Void v) -> { // <- Just imagine some async operation here
                        onDone.accept(null, null);
                    });
                }else{
                    onDone.accept(null, null);
                }
                return iter.hasNext();
            }
            @Override public boolean onError(Throwable ex, Void ctx) {
                testContext.fail(ex);
                return false;
            }
            @Override public void onDone(Void ctx) {
                testContext.assertTrue(!iter.hasNext());
                vertx.setTimer(1, nonsense -> {
                    testContext.assertEquals(availTokens, limit.availablePermits());
                    async.complete();
                });
            }
        });
    }

    @Test
    public void worksForZeroElements(TestContext testContext) {
        Async async = testContext.async();
        int availTokens = limit.availablePermits();
        target.request(limit, null, new UpperBoundParallel.Mentor<Void>() {
            Iterator<String> iter = List.<String>of().iterator();
            @Override public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void unused) {
                if(iter.hasNext()){
                    String elem = iter.next();
                    vertx.runOnContext((Void v) -> { // <- Just imagine some async operation here
                        onDone.accept(null, null);
                    });
                }else{
                    onDone.accept(null, null);
                }
                return iter.hasNext();
            }
            @Override public boolean onError(Throwable ex, Void ctx) {
                testContext.fail(ex);
                return false;
            }
            @Override public void onDone(Void ctx) {
                testContext.assertTrue(!iter.hasNext());
                vertx.setTimer(1, nonsense -> {
                    testContext.assertEquals(availTokens, limit.availablePermits());
                    async.complete();
                });
            }
        });
    }

    @Test
    public void worksIfHandlerThrows(TestContext testContext) {
        Async async = testContext.async();
        RuntimeException myFancyTestException = new RuntimeException() {};
        final int totalPermits = 10;
        final int permitsUsedByOther = 2;
        Semaphore limiter = new Semaphore(totalPermits);

        // took 2 permits, simulator permits used by other
        try {
            limiter.acquire(permitsUsedByOther);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        int availTokens = limiter.availablePermits();
        target.request(limiter, null, new UpperBoundParallel.Mentor<Void>() {
            Iterator<String> iter = List.<String>of("the-lonely-elem-1", "the-lonely-elem-2", "the-lonely-elem-3", "the-lonely-elem-4", "the-lonely-elem-5").iterator();

            @Override
            public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void unused) {
                if (iter.hasNext()) {
                    String value = iter.next();
                    if ("the-lonely-elem-4".equals(value)) {
                        throw myFancyTestException;
                    } else {
                        vertx.runOnContext((Void v) -> { // <- Just imagine some async operation here
                            onDone.accept(null, null);
                        });
                    }
                } else {
                    onDone.accept(null, null);
                }
                return iter.hasNext();
            }

            @Override
            public boolean onError(Throwable ex, Void ctx) {
                testContext.assertEquals(myFancyTestException, ex);
                vertx.setTimer(10, nonsense -> {
                    // all permits I took should be released, but permits from other will not
                    testContext.assertEquals(totalPermits - permitsUsedByOther, limiter.availablePermits());
                    testContext.assertEquals(availTokens, limiter.availablePermits());
                    async.complete();
                });
                return false;
            }

            @Override
            public void onDone(Void ctx) {
                testContext.fail();
            }
        });
    }

    @Test
    public void worksIfHandlerReportsError(TestContext testContext) {
        Async async = testContext.async();
        Throwable myFancyTestException = new Throwable(){};
        int availTokens = limit.availablePermits();
        target.request(limit, null, new UpperBoundParallel.Mentor<Void>() {
            Iterator<String> iter = List.<String>of("the-lonely-elem").iterator();
            @Override public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void unused) {
                if(iter.hasNext()){
                    iter.next();
                    onDone.accept(myFancyTestException, null);
                }
                return iter.hasNext();
            }
            @Override public boolean onError(Throwable ex, Void ctx) {
                testContext.assertEquals(myFancyTestException, ex);
                vertx.setTimer(1, nonsense -> {
                    testContext.assertEquals(availTokens, limit.availablePermits());
                    async.complete();
                });
                return false;
            }
            @Override public void onDone(Void ctx) {
                testContext.fail();
            }
        });
    }

    @Test
    public void mustNotContinueIfDoneNotReported(TestContext testContext) {
        Async async = testContext.async();
        target.request(limit, null, new UpperBoundParallel.Mentor<Void>() {
            @Override public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void unused) {
                // onDone() call missing by intent.
                return false;
            }
            @Override public boolean onError(Throwable ex, Void ctx) {
                testContext.fail();
                return false;
            }
            @Override public void onDone(Void ctx) {
                testContext.fail();
            }
        });
        vertx.setTimer(500, nonsense -> async.complete());
    }

    @Test
    public void reportsErrorIfNoTokensLeft(TestContext testContext) {
        Async async = testContext.async();
        limit.drainPermits(); // <- Whops, no tokens left for code under test.
        target.request(limit, null, new UpperBoundParallel.Mentor<Void>() {
            Iterator<String> iter = List.<String>of("the-lonely-elem").iterator();
            @Override public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void unused) {
                testContext.fail();
                return false;
            }
            @Override public boolean onError(Throwable ex, Void ctx) {
                testContext.assertTrue(ex instanceof ResourceExhaustionException);
                testContext.assertNotNull(ex.getMessage());
                vertx.setTimer(1, nonsense -> {
                    testContext.assertEquals(0, limit.availablePermits());
                    async.complete();
                });
                return false;
            }
            @Override public void onDone(Void ctx) {
                testContext.fail();
            }
        });
    }

    @Test
    public void testSemaphoreAreAllReleasedBeforeOnDoneCall(TestContext testContext) {
        Async async = testContext.async();
        int semaphoreLimit = 3;
        int totalTasks = 10;
        Semaphore limiter = new Semaphore(semaphoreLimit);
        AtomicInteger completedTasks = new AtomicInteger(0);
        UpperBoundParallel parallel = new UpperBoundParallel(vertx, newWastefulExceptionFactory());
        parallel.request(limiter, null, new UpperBoundParallel.Mentor<>() {
            private final AtomicInteger taskCounter = new AtomicInteger(0);

            @Override
            public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Object ctx) {
                int taskId = taskCounter.getAndIncrement();
                System.out.println("runOneMore: " + taskId);
                // Simulate task execution with a slight delay.
                vertx.setTimer(100, id -> {
                    try {
                        System.out.println("Task completed: " + taskId);
                        completedTasks.incrementAndGet();
                    } finally {
                        onDone.accept(null, null); // Mark task as done.
                    }
                });
                return taskId < totalTasks - 1;
            }

            @Override
            public boolean onError(Throwable ex, Object ctx) {
                System.err.println("Error in task: " + ex.getMessage());
                return false; // Stop processing on error.
            }

            @Override
            public void onDone(Object ctx) {
                System.out.println("All tasks completed without error.");
                testContext.assertEquals(totalTasks, completedTasks.get(), "Number of completed tasks should match total tasks.");
                testContext.assertEquals(semaphoreLimit, limiter.availablePermits(), "All semaphore permits should be released.");
                async.complete();
            }
        });
    }


    @Test
    public void worksIfHandlerThrowsAndContinue(TestContext testContext) {
        Async async = testContext.async();
        RuntimeException myFancyTestException = new RuntimeException() {};
        final int totalPermits = 10;
        final int permitsUsedByOther = 2;
        Semaphore limiter = new Semaphore(totalPermits);

        // took 2 permits, simulator permits used by other
        try {
            limiter.acquire(permitsUsedByOther);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        int availTokens = limiter.availablePermits();
        target.request(limiter, null, new UpperBoundParallel.Mentor<Void>() {
            Iterator<String> iter = List.<String>of("the-lonely-elem-1", "the-lonely-elem-2", "the-lonely-elem-3", "the-lonely-elem-4", "the-lonely-elem-5").iterator();

            @Override
            public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void unused) {
                if (iter.hasNext()) {
                    String value = iter.next();
                    if ("the-lonely-elem-4".equals(value)) {
                        throw myFancyTestException;
                    } else {
                        vertx.runOnContext((Void v) -> { // <- Just imagine some async operation here
                            onDone.accept(null, null);
                        });
                    }
                } else {
                    onDone.accept(null, null);
                }
                return iter.hasNext();
            }

            @Override
            public boolean onError(Throwable ex, Void ctx) {
                testContext.assertEquals(myFancyTestException, ex);
                return true;
            }

            @Override
            public void onDone(Void ctx) {
                vertx.setTimer(10, nonsense -> {
                    // all permits I took should be released, but permits from other will not
                    testContext.assertEquals(totalPermits - permitsUsedByOther, limiter.availablePermits());
                    testContext.assertEquals(availTokens, limiter.availablePermits());
                    async.complete();
                });
            }
        });
    }
}
