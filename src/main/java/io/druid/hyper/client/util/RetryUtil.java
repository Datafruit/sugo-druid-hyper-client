package io.druid.hyper.client.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Callable;

public class RetryUtil {
    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);
    public static final int MAX_TRY_TIMES = 3;
    /**
     * Retry an operation using fuzzy exponentially increasing backoff. The wait time after the nth failed attempt is
     * min(60000ms, 1000ms * pow(2, n - 1)), fuzzed by a number drawn from a Gaussian distribution with mean 0 and
     * standard deviation 0.2.
     *
     * If maxTries is exhausted, or if shouldRetry returns false, the last exception thrown by "f" will be thrown
     * by this function.
     *
     * @param action           the operation
     * @param actionBeforeRetry the operation before each retry
     * @param shouldRetry predicate determining whether we should retry after a particular exception thrown by "f"
     * @param quietTries  first quietTries attempts will log exceptions at DEBUG level rather than WARN
     * @param maxTries    maximum number of attempts
     *
     * @return result of the first successful operation
     *
     * @throws Exception if maxTries is exhausted, or shouldRetry returns false
     */
    public static <T, S> T retry(
            final Callable<T> action,
            final Callable<S> actionBeforeRetry,
            Predicate<Throwable> shouldRetry,
            final int quietTries,
            final int maxTries
    ) throws Exception
    {
        Preconditions.checkArgument(maxTries > 0, "maxTries > 0");
        int nTry = 0;
        while (true) {
            try {
                nTry++;
                return action.call();
            }
            catch (Throwable e) {
                log.error("Action failed, and [" + nTry + "] times retries. Error details:", e);
                if (nTry < maxTries && shouldRetry.apply(e)) {
                    awaitNextRetry(e, nTry, nTry <= quietTries);
                    if (actionBeforeRetry != null) {
                        actionBeforeRetry.call();
                    }
                } else {
                    log.error("Action failed, and no more retries. Error details:", e);
                    Throwables.propagateIfInstanceOf(e, Exception.class);
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    /**
     * Same as {@link #retry(Callable, Callable, Predicate, int, int)} with quietTries = 0.
     */
    public static <T, S> T retry(final Callable<T> action, final Callable<S> actionBeforeRetry,
                              Predicate<Throwable> shouldRetry, final int maxTries) throws Exception
    {
        return retry(action, actionBeforeRetry, shouldRetry, 0, maxTries);
    }

    private static void awaitNextRetry(final Throwable e, final int nTry, final boolean quiet) throws InterruptedException
    {
        final long baseSleepMillis = 1000;
        final long maxSleepMillis = 60000;
        final double fuzzyMultiplier = Math.min(Math.max(1 + 0.2 * new Random().nextGaussian(), 0), 2);
        final long sleepMillis = (long) (Math.min(maxSleepMillis, baseSleepMillis * Math.pow(2, nTry - 1))
                * fuzzyMultiplier);

        StringBuilder sb = new StringBuilder();
        sb.append("Failed on try ").append(nTry).append(" ms, retrying in ")
           .append(sleepMillis).append(" ms. Error: ").append(e.getMessage());
        if (quiet) {
            log.info(sb.toString());
        } else {
            log.warn(sb.toString());
        }
        Thread.sleep(sleepMillis);
    }
}
