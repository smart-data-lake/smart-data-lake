/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package io.smartdatalake.util.azure.logging.loganalytics;

import io.smartdatalake.util.azure.LogAnalyticsEnvironment;
import io.smartdatalake.util.azure.client.loganalytics.LogAnalyticsClient;
import io.smartdatalake.util.azure.client.loganalytics.LogAnalyticsSendBufferClient;
import io.smartdatalake.util.azure.logging.JSONLayout;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import static io.smartdatalake.util.azure.logging.JSONLayout.TIMESTAMP_FIELD_NAME;

/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected by its corresponding MIT license
 */
public class LogAnalyticsAppender extends AppenderSkeleton {
    private static final String LA_SPARKLOGGINGEVENT_NAME_REGEX=System.getenv().getOrDefault("LA_SPARKLOGGINGEVENT_NAME_REGEX", "");
    private static final String LA_SPARKLOGGINGEVENT_MESSAGE_REGEX=System.getenv().getOrDefault("LA_SPARKLOGGINGEVENT_MESSAGE_REGEX", "");

    private static final Filter DEFAULT_FILTER = new Filter() {
        @Override
        public int decide(LoggingEvent loggingEvent) {
            String loggerName=loggingEvent.getLoggerName();
            // ignore logs from org.apache.http to avoid infinite loop on logger error
            if (loggerName.startsWith("org.apache.http")) {
                return Filter.DENY;
            }
            // if LA_SPARKLOGGINGEVENT_NAME_REGEX is not empty, deny logs where the name doesn't match the regex
            if (!LA_SPARKLOGGINGEVENT_NAME_REGEX.isEmpty() && !loggerName.matches(LA_SPARKLOGGINGEVENT_NAME_REGEX)) {
                return Filter.DENY;
            }
            // if LA_SPARKLOGGINGEVENT_MESSAGE_REGEX is not empty, deny logs where the message doesn't match the regex
            if (!LA_SPARKLOGGINGEVENT_MESSAGE_REGEX.isEmpty() && !loggingEvent.getRenderedMessage().matches(LA_SPARKLOGGINGEVENT_MESSAGE_REGEX)) {
                return Filter.DENY;
            }

            return Filter.NEUTRAL;
        }
    };

    private static final String DEFAULT_LOG_TYPE = "SparkLoggingEvent";
    // We will default to environment so the properties file can override
    private String workspaceId = LogAnalyticsEnvironment.getWorkspaceId();
    private String secret = LogAnalyticsEnvironment.getWorkspaceKey();
    private String logType = DEFAULT_LOG_TYPE;
    private LogAnalyticsSendBufferClient client;

    public LogAnalyticsAppender() {
        this.addFilter(DEFAULT_FILTER);
        // Add a default layout so we can simplify config
        this.setLayout(new JSONLayout());
    }

    @Override
    public void activateOptions() {
        this.client = new LogAnalyticsSendBufferClient(
                new LogAnalyticsClient(this.workspaceId, this.secret),
                this.logType
        );
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        try {
            String json = this.getLayout().format(loggingEvent);
            this.client.sendMessage(json, TIMESTAMP_FIELD_NAME);
        } catch (Exception ex) {
            LogLog.error("Error sending logging event to Log Analytics", ex);
        }
    }

    @Override
    public boolean requiresLayout() {
        // We will set this to false so we can simplify our config
        // If no layout is provided, we will get the default.
        return false;
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public void setLayout(Layout layout) {
        // This will allow us to configure the layout from properties to add custom JSON stuff.
        if (!(layout instanceof JSONLayout)) {
            throw new UnsupportedOperationException("layout must be an instance of JSONLayout");
        }

        super.setLayout(layout);
    }

    @Override
    public void clearFilters() {
        super.clearFilters();
        // We need to make sure to add the filter back so we don't get stuck in a loop
        this.addFilter(DEFAULT_FILTER);
    }

    public String getWorkspaceId() {
        return this.workspaceId;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getSecret() {
        return this.secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getLogType() {
        return this.logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }
}
