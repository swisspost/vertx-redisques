package org.swisspush.redisques.performance;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * We still can utilize parallelity without assuming an infinite amount
 * of resources. A KISS approach to do this, is to apply an upper bound
 * to what we do in parallel. And thats what this class tries to assist
 * with. It wants to be that tool that allows parallelity but maintains
 * upper bounds. For stone-age programmers: It's nothing else than a
 * semaphore really. But decorated with a vertx gift bow to make it fit
 * better in the new framework world ;)
 *
 * Long story:
 *
 * Level 1: KISS says do everything sequentially. Stop here! You're
 * done! "Optimization is the root of all evil" you know? Happy you. So
 * just do NOT use this class if this is the case for your use case.
 *
 * Level 2: There are cases where we do not have the time to do
 * everything sequentially. The next step is to go parallel and/or
 * concurrent (I don't care which term you think is the correct one.
 * Just take the one that fits your opinion better). But by going
 * parallel, we just assume we have an infinite amount of resources
 * available (eg sockets, memory, CPU time, ...), so we can happily fill
 * queues to an infinite number of entries without running in trouble
 * ever. In case your resources are infinite, happy you you're done.
 *
 * Level 3: Welcome in my world. Where reality starts to hit you sooner or
 * later and you'll realize no matter how much of those "infinite cloud
 * resources" and fancy frameworks magic you throw at your problem, it won't
 * solve the issue. Performance-is-not-an-issue? Please, just go back to
 * "Level 1" and be happy there.
 */
public class UpperBoundParallel {

    private static final Logger log = getLogger(UpperBoundParallel.class);
    private static final long RETRY_DELAY_IF_LIMIT_REACHED_MS = 8;
    private final Vertx vertx;
    private final RedisQuesExceptionFactory exceptionFactory;

    public UpperBoundParallel(Vertx vertx, RedisQuesExceptionFactory exceptionFactory) {
        assert vertx != null;
        this.vertx = vertx;
        this.exceptionFactory = exceptionFactory;
    }

    public <Ctx> void request(Semaphore limit, Ctx ctx, Mentor<Ctx> mentor) {
        var req = new Request<>(ctx, mentor, limit);
        resume(req);
    }

    private <Ctx> void resume(Request<Ctx> req) {
        if (!req.lock.tryLock()) {
            log.trace("Some other thread already working here");
            return;
        } else try {
            Thread ourself = currentThread();
            if (req.worker == null) {
                log.trace("worker := ourself");
                req.worker = ourself;
            } else if (req.worker != ourself) {
                log.trace("Another thread is already working here");
                return;
            }
            // Enqueue as much we can.
            while (true) {
                if (req.isFatalError) {
                    log.trace("return from 'resume()' because isFatalError");
                    return;
                }
                if (!req.hasMore) {
                    if (req.numInProgress == 0 && !req.isDoneCalled) {
                        req.isDoneCalled = true;
                        // give up lock because we don't know how much time mentor will use.
                        req.lock.unlock();
                        log.trace("call 'mentor.onDone()'");
                        try {
                            req.mentor.onDone(req.ctx);
                        } finally {
                            req.lock.lock(); // MUST get back our lock RIGHT NOW.
                        }
                    } else {
                        log.trace("return for now (hasMore = {}, numInProgress = {})", req.hasMore, req.numInProgress);
                    }
                    return;
                }
                if (req.numTokensAvailForOurself > 0) {
                    // We still have a token reserved for ourself. Use those first before acquiring
                    // new ones. Explanation see comment in 'onOneDone()'.
                    assert req.numTokensAvailForOurself == 1 : "TODO is this always true? "+ req.numTokensAvailForOurself;
                    req.numTokensAvailForOurself -= 1;
                }else if (!req.limit.tryAcquire()) {
                    log.debug("redis request limit reached. Need to pause now.");
                    break; // Go to end of loop to schedule a run later.
                }
                req.hasStarted = true;
                req.numInProgress += 1;
                boolean hasMore = true;
                try {
                    // We MUST give up our lock while calling mentor. We cannot know how long
                    // mentor is going to block (which would then cascade to all threads
                    // waiting for our lock).
                    req.lock.unlock();
                    log.trace("mentor.runOneMore()  numInProgress={}", req.numInProgress);
                    hasMore = req.mentor.runOneMore(new BiConsumer<>() {
                        // this boolean is just for paranoia, in case mentor tries to call back too often.
                        final AtomicBoolean isCalled = new AtomicBoolean();
                        @Override public void accept(Throwable ex, Void ret) {
                            if (!isCalled.compareAndSet(false, true)) return;
                            onOneDone(req, ex);
                        }
                    }, req.ctx);
                } catch (RuntimeException ex) {
                    onOneDone(req, ex);
                } finally {
                    // We MUST get back our lock right NOW. No way to just 'try'.
                    log.trace("mentor.runOneMore() -> hasMore={}", hasMore);
                    req.lock.lock();
                    req.hasMore = hasMore;
                }
            }
            assert req.numInProgress >= 0 : req.numInProgress;
            if (req.numInProgress == 0) {
                if (!req.hasStarted) {
                    // We couldn't even trigger one single task. No resources available to
                    // handle any more requests. This caller has to try later.
                    req.isFatalError = true;
                    Exception ex = exceptionFactory.newResourceExhaustionException(
                            "No more resources to handle yet another request now.", null);
                    req.mentor.onError(ex, req.ctx);
                    return;
                }else{
                    // Why couldn't we fire a single event this turn? Try later.
                    assert false : "TODO can this happen?";
                    vertx.setTimer(RETRY_DELAY_IF_LIMIT_REACHED_MS, nonsense -> resume(req));
                }
            }
        } finally {
            req.worker = null;
            req.lock.unlock();
        }
    }

    private <Ctx> void onOneDone(Request<Ctx> req, Throwable ex) {
        req.lock.lock();
        try {
            // Do NOT release that token yet. instead mark the token as "ready-to-be-used".
            // To signalize 'resume()' that we do not need to 'acquire' another token from
            // 'limit' and we instead can re-use that one we acquired earlier.
            // Reasoning:
            // Originally we did just 'release' the token back to the pool and then acquired
            // another one later in 'resume()'. But this is problematic, as in this case we
            // give yet more incoming requests a chance to also start their processing.
            // Which in the end runs us into resource exhaustion. Because we will start more
            // and more requests and have no tokens free to complete the already running
            // requests. So by keeping that token we already got reserved to ourself, we
            // can apply backpressure to new incoming requests. This allows us to complete
            // the already running requests.
            req.numInProgress -= 1;
            req.numTokensAvailForOurself += 1;
            // ^^-- Token transfer only consists of those two statements.
            log.trace("onOneDone({})  {} remaining", ex != null ? "ex" : "null", req.numInProgress);
            assert req.numInProgress >= 0 : req.numInProgress + " >= 0  (BTW: mentor MUST call 'onDone' EXACTLY once)";
            boolean isFatalError = true;
            if (ex != null) try {
                // Unlock, to prevent thread stalls as we don't know for how long mentor
                // is going to block.
                req.lock.unlock();
                if (log.isDebugEnabled()) {
                    log.debug("mentor.onError({}: {})", ex.getClass().getName(), ex.getMessage());
                }
                isFatalError = !req.mentor.onError(ex, req.ctx);
            } finally {
                req.lock.lock(); // Need our lock back.
                req.isFatalError = isFatalError;
            }
        } finally {
            req.lock.unlock();
            vertx.runOnContext(nonsense -> resume(req));
        }
    }

    private final class Request<Ctx> {
        private final Ctx ctx;
        private final Mentor<Ctx> mentor;
        private final Lock lock = new ReentrantLock();
        private final Semaphore limit;
        private Thread worker = null;
        private int numInProgress = 0;
        private int numTokensAvailForOurself = 0;
        private boolean hasMore = true;
        private boolean hasStarted = false; // true, as soon we could start at least once.
        private boolean isFatalError = false;
        private boolean isDoneCalled = false;

        private Request(Ctx ctx, Mentor<Ctx> mentor, Semaphore limit) {
            this.ctx = ctx;
            this.mentor = mentor;
            this.limit = limit;
        }
    }


    public static interface Mentor<Ctx> {

        /**
         * Gets called as many times as possible until specified limit is
         * reached. More calls are triggered as soon some tasks call 'onDone' to
         * signalize they've completed.
         *
         * @param onDone
         *      MUST be called exactly ONCE as soon the requested task has
         *      completed its execution.
         * @return true if more elements have to be processed. False if
         *      iteration source has reached its end.
         */
        boolean runOneMore(BiConsumer<Throwable, Void> onDone, Ctx ctx);

        /**
         * @return true if iteration should continue with other elements. False
         *      if no more elements should be processed.
         */
        boolean onError(Throwable ex, Ctx ctx);

        /**
         * Called once as soon the iteration has ended SUCCESSFULLY. It is NOT
         * called if {@link #onError(Throwable, Object)} did request to STOP the
         * iteration for example.
         */
        void onDone(Ctx ctx);
    }


}
