package io.smartdatalake.util.misc

import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import java.util.{Timer, TimerTask}

import sun.misc.SharedSecrets

import scala.collection.JavaConverters._

/**
 * Utils to monitor memory usage and shutdown cause
 */
object MemoryUtils extends SmartDataLakeLogger {

  private lazy val timer = new Timer()
  private var timerStarted = false

  /**
   * Start a timer to periodically log memory usage
   */
  def startMemoryLogger(intervalSec: Int, logLinuxMem: Boolean = false, logLinuxCgroupMem: Boolean = false, logBuffers: Boolean = false): Unit = {
    timer.scheduleAtFixedRate(new MemoryLogTimerTask(logLinuxMem, logLinuxCgroupMem, logBuffers), 0, intervalSec * 1000)
    timerStarted = true
    logger.info("Memory logger timer task started.")
  }
  def stopMemoryLogger(): Unit = {
    if (timerStarted) {
      timer.cancel()
      logger.info("Memory logger timer task stopped.")
    }
  }

  private class MemoryLogTimerTask(logLinuxMem: Boolean, logLinuxCgroupMem: Boolean, logBuffers: Boolean) extends TimerTask {
    override def run(): Unit = {
      logHeapInfo( !isWindowsOS && logLinuxMem, !isWindowsOS && logLinuxCgroupMem, logBuffers)
    }
  }

  def logHeapInfo(logLinuxMem: Boolean, logLinuxCgroupMem: Boolean, logBuffers: Boolean): Unit = {
    val memStats = getMemoryUtilization ++ getThreadUtilization ++
      (if(logBuffers) getDirectBufferPools ++ getMappedBufferPools else Seq()) ++
      (if(logLinuxMem) getLinuxMem ++ getCGroupMem else Seq())
    logger.info(s"memory info ${memStats.map{ case (k,v) => s"$k=$v"}.mkString(", ")}")
    if(logLinuxCgroupMem) logger.debug("cgroup memory statistics", getCGroupMemStat)
  }

  def logHeapInfoLegacy(): Unit = {
    val total = Runtime.getRuntime.totalMemory()/1024/1024
    val free = Runtime.getRuntime.freeMemory()/1024/1024
    val max = Runtime.getRuntime.maxMemory()/1024/1024
    val used = total - free
    val available = max - used
    logger.info(s"heap info: used=${used}MB, max=${max}MB")
  }

  private lazy val memoryMXBean = ManagementFactory.getMemoryMXBean
  private lazy val threadMXBean = ManagementFactory.getThreadMXBean
  private lazy val bufferPools = ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean])
  private lazy val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)

  private def getMemoryUtilization: Seq[(String,Any)] = {
    val heap = memoryMXBean.getHeapMemoryUsage
    val nonHeap = memoryMXBean.getNonHeapMemoryUsage
    Seq(
      "heapMemoryUsed" -> formatBytesMB(heap.getUsed),
      "heapMemoryCommitted" -> formatBytesMB(heap.getCommitted),
      "heapMemoryMax" -> formatBytesMB(heap.getMax),
      "nonheapMemoryUsed" -> formatBytesMB(nonHeap.getUsed),
      "nonheapMemoryCommitted" -> formatBytesMB(nonHeap.getCommitted),
      "nonheapMemoryMax" -> formatBytesMB(nonHeap.getMax)
    )
  }

  private def getDirectBufferPools: Seq[(String,Any)] = {
    bufferPools.asScala
    .find(_.getName=="direct")
    .map( bean => Seq(
      "directBuffersCount" -> bean.getCount,
      "directBuffersUsed" -> s"${bean.getMemoryUsed/1024/1024}MB",
      "directBuffersCapacity" -> s"${bean.getTotalCapacity/1024/1024}MB"
    )).getOrElse(Seq())
  }

  private def getMappedBufferPools: Seq[(String,Any)] = {
    bufferPools.asScala
    .find(_.getName=="mapped")
    .map( bean => Seq(
      "mappedBuffersCount" -> bean.getCount,
      "mappedBuffersUsed" -> formatBytesMB(bean.getMemoryUsed),
      "mappedBuffersCapacity" -> formatBytesMB(bean.getTotalCapacity)
    )).getOrElse(Seq())
  }

  private def getThreadUtilization: Seq[(String,Any)] = {
    val totalThreads = threadMXBean.getThreadCount
    val daemonThreads = threadMXBean.getDaemonThreadCount
    Seq(
      "totalThreads" -> totalThreads,
      "daemonThreads" -> daemonThreads
    )
  }

  private def getCGroupMem: Seq[(String,Any)] = try {
    def getCGroupMem( key: String ) = {
      val commandString = s"cat /sys/fs/cgroup/memory/$key"
      val cmd = Array("/bin/sh", "-c", commandString)
      val p = Runtime.getRuntime.exec(cmd)
      val result = scala.io.Source.fromInputStream(p.getInputStream)
      result.getLines().toSeq.head.toLong
    }
    val limit = getCGroupMem("memory.limit_in_bytes")
    val usage = getCGroupMem("memory.usage_in_bytes")
    Seq(
      "cGroupLimit" -> formatBytesMB(limit),
      "cGroupUsage" -> formatBytesMB(usage)
    )
  } catch {
    case e: Exception =>
      logger.warn("could not get cGroup process memory: "+e.getMessage)
      Seq()
  }

  private def getCGroupMemStat: Map[String,String] = {
    val commandString = s"cat /sys/fs/cgroup/memory/memory.stat"
    val cmd = Array("/bin/sh", "-c", commandString)
    val p = Runtime.getRuntime.exec(cmd)
    val result = scala.io.Source.fromInputStream(p.getInputStream)
    result.getLines().toSeq.map(_.split(" ")).filter(_.size>=2).map( v => v(0)->v(1)).toMap
  }


  private def getLinuxMem: Seq[(String,Any)] = try {
    val commandString = s"ps -p $pid u"
    val cmd = Array("/bin/sh", "-c", commandString)
    val p = Runtime.getRuntime.exec(cmd)
    val result = scala.io.Source.fromInputStream(p.getInputStream)
    val resultParsedLines = result.getLines().toSeq.map(_.toLowerCase.split(" ").filter(!_.isEmpty))
    val resultParsed = resultParsedLines.head.zip(resultParsedLines(1)).toMap
    val vsz = resultParsed("vsz").toLong
    val rss = resultParsed("rss").toLong
    val pctMem = resultParsed("%mem").toFloat
    Seq(
      "linuxMemoryVsz" -> formatBytesMB(vsz*1024),
      "linuxMemoryRss" -> formatBytesMB(rss*1024),
      "linuxPctMem" -> pctMem
    )
  } catch {
    case e: Exception =>
      logger.warn("could not get linux process memory: "+e.getMessage)
      Seq()
  }

  /**
   * Add shutdown hooks to trace why the java application has stopped
   */
  def addDebugShutdownHooks(): Unit = {

    // add runtime shutdown hook to log memory and stacktrace
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        logger.info(s"app shutdown hook: thread=${Thread.currentThread}, stacktrace=${new Exception().getStackTrace.map(_.toString)}")
        logHeapInfo(true, false, false)
      }
    })

    // try to add system shutdown hook to log stacktrace
    try {
      SharedSecrets.getJavaLangAccess.registerShutdownHook(7, true, new Runnable {
        def run(): Unit = {
          logger.info(s"jvm shutdown hook: thread=${Thread.currentThread}, stacktrace=${new Exception().getStackTrace.map(_.toString)}")
        }
      })
    } catch {
      case e:Exception => logger.warn(s"could not create system shutdown hook: ${e.getClass.getSimpleName} - ${e.getMessage}")
    }
  }

  def formatBytesMB(bytes:Long): String = {
    val megaBytes = BigDecimal.decimal(bytes.toFloat / 1024 / 1024).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    s"${megaBytes}MB"
  }

  def isWindowsOS: Boolean = {
    sys.env.get("OS").exists(_.toLowerCase.startsWith("windows"))
  }
}
