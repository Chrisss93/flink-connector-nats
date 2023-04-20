package com.github.chrisss93.connector.nats.source.reader.fetcher;

import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;

interface NoWakeSplitFetcherTask extends SplitFetcherTask {
    @Override
    default void wakeUp() {}
}
