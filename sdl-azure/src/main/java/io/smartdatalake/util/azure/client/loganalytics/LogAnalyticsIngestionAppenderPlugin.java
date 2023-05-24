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
 * Log4j2 Appender writing logs to Azure Monitor / LogAnalytics using "Log Ingestion API",
 * see also https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview
 * This API needs an Azure AD Token for authentication, e.g. an applicationId/Secret,
 * and connects to an Azure Monitor Data Collector Endpoint and a Data Collector Rule to create logs in a custom table in LogAnalytics.
 * The schema of table must be defined when creating the Data Collector Rule, which also allows to do some parsing/transformations.
 *
 * For sending SDLB logs to AzureLogAnalytics, you can use the bundled JsonTemplateLayout definition `LogAnalyticsSdlbLayout.json`.
 * Log4j2 configuration example: ```
 *     LogAnalyticsIngestion:
 *       name: AzureLogAnalytics
 *       endpoint: "https://???.switzerlandnorth-1.ingest.monitor.azure.com"
 *       ruleId: "dcr-..."
 *       streamName: "Custom-sdlb-log"
 *       JsonTemplateLayout:
 *         eventTemplateUri: "classpath:LogAnalyticsSdlbLayout.json"
 *         locationInfoEnabled: "true"
 * ```
 * There is also a bundled file `LogAnalyticsSdlbLayout.json.sample` with example log messages for creating the Data Collector Rule.
 * To authenticate with Azure AD define environment variables AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET
 *
 * Implementing this Appender in Java is needed as there is no static method in scala that can be annotated for Log4j2 with @PluginBuilderFactory.
 * The implementation is a wrapper around LogAnalyticsAppender implemented in Scala.
 */
@Plugin(name = "LogAnalyticsIngestion", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class LogAnalyticsIngestionAppenderPlugin extends AbstractAppender {

    private LogAnalyticsAppender impl = null;

    private LogAnalyticsIngestionAppenderPlugin(String name, String endpoint, String ruleId, String streamName, Integer maxDelayMillis, Integer batchSize, Layout<? extends Serializable> layout, Filter filter) {
        super(name, filter, layout, /*ignoreExceptions*/ false, new Property[]{});
        impl = LogAnalyticsAppender$.MODULE$.createIngestionAppender(getName(), endpoint, ruleId, streamName, maxDelayMillis, batchSize, getLayout(), getFilter());
    }

    @PluginBuilderFactory
    public static <B extends LogAnalyticsIngestionAppenderPlugin.Builder<B>> B newBuilder() {
        return new LogAnalyticsIngestionAppenderPlugin.Builder<B>().asBuilder();
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

    public static class Builder<B extends LogAnalyticsIngestionAppenderPlugin.Builder<B>> extends AbstractAppender.Builder<B>
            implements org.apache.logging.log4j.core.util.Builder<LogAnalyticsIngestionAppenderPlugin> {

        @PluginBuilderAttribute
        @Required
        private String endpoint;

        @PluginBuilderAttribute(sensitive = true)
        @Required
        private String ruleId;

        @PluginBuilderAttribute(sensitive = true)
        @Required
        private String streamName;

        @PluginBuilderAttribute
        private Integer maxDelayMillis = 1000;

        @PluginBuilderAttribute
        private Integer batchSize = 100;

        public B setEndpoint(final String endpoint) {
            this.endpoint = endpoint;
            return asBuilder();
        }

        public B setRuleId(final String ruleId) {
            this.ruleId = ruleId;
            return asBuilder();
        }
        public B setStreamName(final String streamName) {
            this.streamName = streamName;
            return asBuilder();
        }

        public B setMaxDelayMillis(final Integer maxDelayMillis) {
            this.maxDelayMillis = maxDelayMillis;
            return asBuilder();
        }

        public B setBatchSize(final Integer batchSize) {
            this.batchSize = batchSize;
            return asBuilder();
        }

        @Override
        public LogAnalyticsIngestionAppenderPlugin build() {
            return new LogAnalyticsIngestionAppenderPlugin(getName(), endpoint, ruleId, streamName, maxDelayMillis, batchSize, getLayout(), getFilter());
        }
    }
}
