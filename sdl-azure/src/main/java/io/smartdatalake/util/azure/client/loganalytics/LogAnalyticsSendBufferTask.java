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

package io.smartdatalake.util.azure.client.loganalytics;

import io.smartdatalake.util.azure.client.GenericSendBufferTask;

import java.util.List;

/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected by its corresponding MIT license
 */
public class LogAnalyticsSendBufferTask extends GenericSendBufferTask<String> {

    private final LogAnalyticsClient client;
    private final String logType;
    private final String timeGeneratedField;

    public LogAnalyticsSendBufferTask(LogAnalyticsClient client,
                                      String logType,
                                      String timeGenerateField,
                                      int maxBatchSizeBytes,
                                      int batchTimeInMilliseconds
    ) {
        super(maxBatchSizeBytes, batchTimeInMilliseconds);
        this.client = client;
        this.logType = logType;
        this.timeGeneratedField = timeGenerateField;
    }

    @Override
    protected int calculateDataSize(String data) {
        return data.getBytes().length;
    }

    @Override
    protected void process(List<String> datas) {
        if (datas.isEmpty()) {
            return;
        }

        // Build up Log Analytics "batch" and send.
        // How should we handle failures?  I think there is retry built into the HttpClient,
        // but what if that fails as well?  I suspect we should just log it and move on.

        // We are going to assume that the events are properly formatted
        // JSON strings.  So for now, we are going to just wrap brackets around
        // them.
        StringBuffer sb = new StringBuffer("[");
        for (String data : datas) {
            sb.append(data).append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(",")).append("]");
        try {
            client.send(sb.toString(), logType, timeGeneratedField);
        } catch (Exception ioe) {
            // We can't do much here since we might be inside a logger
            System.err.println(ioe.getMessage());
            System.err.println(ioe);
        }
    }
}
