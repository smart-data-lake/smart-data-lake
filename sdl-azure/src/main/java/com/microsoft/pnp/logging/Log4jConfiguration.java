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

import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;

public class Log4jConfiguration {
    public static void configure(String configFilename) {
        PropertyConfigurator.configure(configFilename);
    }

    public static void configure(InputStream inputStream) {
        PropertyConfigurator.configure(inputStream);
    }
}
