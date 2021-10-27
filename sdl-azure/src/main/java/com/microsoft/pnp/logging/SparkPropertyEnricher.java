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

package com.microsoft.pnp.logging;

import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import com.microsoft.pnp.SparkInformation;

import java.util.Map;

import scala.collection.JavaConverters;

public class SparkPropertyEnricher extends Filter {

    @Override
    public int decide(LoggingEvent loggingEvent) {
        // This is not how we should really do this since we aren't actually filtering,
        // but because Spark uses the log4j.properties configuration instead of the XML
        // configuration, our options are limited.

        // There are some things that are unavailable until a certain point
        // in the Spark lifecycle on the driver.  We will try to get as much as we can.
        Map<String, String> javaMap = ((Map<String, String>) JavaConverters
                .mapAsJavaMapConverter(SparkInformation.get()));
        for (Map.Entry<String, String> entry : javaMap.entrySet()) {
            loggingEvent.setProperty(entry.getKey(), entry.getValue());
        }

        return Filter.NEUTRAL;
    }
}
