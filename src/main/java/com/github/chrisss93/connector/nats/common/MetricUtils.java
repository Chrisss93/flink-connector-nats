package com.github.chrisss93.connector.nats.common;

import io.nats.client.Connection;
import io.nats.client.api.ServerInfo;
import org.apache.flink.metrics.MetricGroup;

public class MetricUtils {

    private static final String SERVER_INFO_GROUP = "serverInfo";

    private static final String SERVER_ID_GAUGE = "serverId";
    private static final String SERVER_NAME_GAUGE = "serverName";
    private static final String SERVER_VERSION_GAUGE = "serverVersion";
    private static final String SERVER_ADDRESS_GAUGE = "serverAddress";
    private static final String CLIENT_ID_GAUGE = "clientId";

    public static final String STATS_GROUP = "statistics";
    public static final String ACK_SUCCESS_COUNTER = "ackSuccess";
    public static final String ACK_FAILURE_COUNTER = "ackFailure";
    public static final String OUTSTANDING_REQUEST_FUTURES = "OutstandingRequestFutures";
    public static final String DUPLICATE_REPLIES_RECEIVED = "DuplicateRepliesReceived";
    public static final String RECONNECTS = "Reconnects";

    public static MetricGroup setServerMetrics(Connection connection, MetricGroup metricGroup) {
        MetricGroup group = metricGroup.addGroup(SERVER_INFO_GROUP);
        ServerInfo info = connection.getServerInfo();
        group.gauge(SERVER_ID_GAUGE, info::getServerId);
        group.gauge(SERVER_NAME_GAUGE, info::getServerName);
        group.gauge(SERVER_VERSION_GAUGE, info::getVersion);
        group.gauge(SERVER_ADDRESS_GAUGE, () -> info.getHost() + ":" + info.getPort());
        group.gauge(CLIENT_ID_GAUGE, info::getClientId);
        return group;
    }
}
