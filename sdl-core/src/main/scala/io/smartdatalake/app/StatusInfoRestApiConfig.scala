package io.smartdatalake.app

/**
 *
 * @param port: port with which the first connection attempt is made
 * @param maxPortRetries: If port is already in use, RestAPI will increment port by one and try with that new port.
 * maxPortRetries describes how many times this should be attempted. If set to 0 it will not be attempted.
 * @param stopOnEnd: Set to false if the RestAPI should remain online even after SDL has finished its execution.
 * In that case, the Application needs to be stopped manually. Useful for debugging.
 */
case class StatusInfoRestApiConfig(port: Int = 4440, maxPortRetries: Int = 10, stopOnEnd: Boolean = false)