package org.swisspush.redisques.migration.tasks;

import io.vertx.core.Future;

public interface Task {
    public String getTaskKey();
    public Future<Boolean> run();
}
