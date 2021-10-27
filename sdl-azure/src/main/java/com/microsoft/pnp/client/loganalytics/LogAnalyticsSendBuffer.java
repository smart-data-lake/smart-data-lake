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

package com.microsoft.pnp.client.loganalytics;

import com.microsoft.pnp.client.GenericSendBuffer;
import com.microsoft.pnp.client.GenericSendBufferTask;

public class LogAnalyticsSendBuffer extends GenericSendBuffer<String> {
    // We will leave this at 25MB, since the Log Analytics limit is 30MB.
    public static final int DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES = 1024 * 1024 * 25;
    public static final int DEFAULT_BATCH_TIME_IN_MILLISECONDS = 5000;

    private final LogAnalyticsClient client;
    private final String logType;
    private final String timeGeneratedField;

    public LogAnalyticsSendBuffer(
            LogAnalyticsClient client,
            String logType,
            String timeGenerateField) {
        super();
        this.client = client;
        this.logType = logType;
        this.timeGeneratedField = timeGenerateField;
    }

    @Override
    protected GenericSendBufferTask<String> createSendBufferTask() {
        return new LogAnalyticsSendBufferTask(
                this.client,
                this.logType,
                this.timeGeneratedField,
                DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES,
                DEFAULT_BATCH_TIME_IN_MILLISECONDS
        );
    }
}
