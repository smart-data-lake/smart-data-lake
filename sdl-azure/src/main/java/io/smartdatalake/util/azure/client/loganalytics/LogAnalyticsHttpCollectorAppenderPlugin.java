/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.azure.client.loganalytics;

import io.smartdatalake.util.azure.LogAnalyticsAppender;
import io.smartdatalake.util.azure.LogAnalyticsAppender$;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;

import java.io.Serializable;

/**
 * Log4j2 Appender writing logs to Azure Monitor / LogAnalytics using "HTTP Data Collector API",
 * see also https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview.
 * This API needs a WorkspaceId/Key for authentication, and creates a custom table in LogAnalytics named "<logType>_CL".
 * The schema of the table is handled through automatically created custom fields.
 *
 * It seems that "HTTP Data Collector API" is no longer very well supported, as they become "Legacy Custom Logs" in LogAnalytics.
 * See [[LogAnalyticsIngestionAppenderPlugin]] for the potential successor and the following blog post: https://devblogs.microsoft.com/azure-sdk/out-with-the-rest-azure-monitor-ingestion-libraries-appear/.
 *
 * Implementing this in Java is needed as there is no static method in scala that can be annotated for Log4j2 with @PluginBuilderFactory.
 * The implementation is a wrapper around LogAnalyticsAppender implemented in Scala.
 */
@Plugin(name = "LogAnalyticsHttpCollector", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class LogAnalyticsHttpCollectorAppenderPlugin extends AbstractAppender {

    private LogAnalyticsAppender impl = null;

    private LogAnalyticsHttpCollectorAppenderPlugin(String name, String workspaceId, String workspaceKey, Integer maxDelayMillis, String logType, Layout<? extends Serializable> layout, Filter filter) {
        super(name, filter, layout, /*ignoreExceptions*/ false, new Property[]{});
        impl = LogAnalyticsAppender$.MODULE$.createHttpCollectorAppender(getName(), workspaceId, workspaceKey, maxDelayMillis, logType, getLayout(), getFilter());
    }

    @PluginBuilderFactory
    public static <B extends LogAnalyticsHttpCollectorAppenderPlugin.Builder<B>> B newBuilder() {
        return new LogAnalyticsHttpCollectorAppenderPlugin.Builder<B>().asBuilder();
    }

    @Override
    public void append(LogEvent event) {
        impl.append(event);
    }

    @Override
    public void start() {
        super.start();
        impl.start();
    }

    @Override
    public void stop() {
        impl.stop();
        super.stop();
    }

    public static class Builder<B extends LogAnalyticsHttpCollectorAppenderPlugin.Builder<B>> extends AbstractAppender.Builder<B>
            implements org.apache.logging.log4j.core.util.Builder<LogAnalyticsHttpCollectorAppenderPlugin> {

        @PluginBuilderAttribute
        @Required
        private String workspaceId;

        @PluginBuilderAttribute(sensitive = true)
        @Required
        private String workspaceKey;

        @PluginBuilderAttribute
        private Integer maxDelayMillis = 1000;

        @PluginBuilderAttribute
        private String logType = "SDLB";

        public B setWorkspaceId(final String workspaceId) {
            this.workspaceId = workspaceId;
            return asBuilder();
        }

        public B setWorkspaceKey(final String workspaceKey) {
            this.workspaceKey = workspaceKey;
            return asBuilder();
        }

        public B setMaxDelayMillis(final Integer maxDelayMillis) {
            this.maxDelayMillis = maxDelayMillis;
            return asBuilder();
        }

        public B setLogType(final String logType) {
            this.logType = logType;
            return asBuilder();
        }

        @Override
        public LogAnalyticsHttpCollectorAppenderPlugin build() {
            return new LogAnalyticsHttpCollectorAppenderPlugin(getName(), workspaceId, workspaceKey, maxDelayMillis, logType, getLayout(), getFilter());
        }
    }
}
