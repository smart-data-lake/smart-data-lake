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

import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MDCCloseableFactory {
    private class MDCCloseable implements AutoCloseable {
        @SuppressWarnings("unchecked")
        public MDCCloseable(Map<String, Object> mdc) {
            // Log4j supports Map<String, Object>, but slf4j wants Map<String, String>
            // Because of type erasure, this should be okay.
            MDC.setContextMap((Map)mdc);
        }

        @Override
        public void close() {
            MDC.clear();
        }
    }

    private Optional<Map<String, Object>> context;

    public MDCCloseableFactory() {
        this(null);
    }

    public MDCCloseableFactory(Map<String, Object> context) {
        this.context = Optional.ofNullable(context);
    }

    public AutoCloseable create(Map<String, Object> mdc) {
        // Values in mdc will override context
        Map<String, Object> newMDC = new HashMap<>();
        this.context.ifPresent(c -> newMDC.putAll(c));
        newMDC.putAll(mdc);
        return new MDCCloseable(newMDC);
    }
}
