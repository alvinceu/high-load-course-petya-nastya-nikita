package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore

class AccountWorker(
    private val properties: PaymentAccountProperties,
    private val adapter: PaymentExternalSystemAdapter,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(AccountWorker::class.java)
    }

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val parallelSemaphore = Semaphore(properties.parallelRequests)

    private val executor = Executors.newFixedThreadPool(
        properties.parallelRequests,
        NamedThreadFactory("account-${properties.accountName}")
    )

    private val queue = LinkedBlockingQueue<PaymentTask>(10_000)

    data class PaymentTask(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long,
    )

    fun trySubmit(task: PaymentTask): Boolean {
        return queue.offer(task)
    }

    fun start() {
        Thread {
            while (!Thread.currentThread().isInterrupted) {
                try {
                    val task = queue.take()
                    processTask(task)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    break
                } catch (e: Exception) {
                    logger.error("Error in AccountWorker for ${properties.accountName}", e)
                }
            }
        }.apply { name = "worker-loop-${properties.accountName}" }.start()
    }

    private fun processTask(task: PaymentTask) {
        val now = System.currentTimeMillis()
        if (task.deadline > 0 && now > task.deadline) {
            logger.warn("[${properties.accountName}] Payment ${task.paymentId} expired (deadline passed)")
            return
        }

        parallelSemaphore.acquire()

        rateLimiter.tickBlocking()

        try {
            executor.submit {
                try {
                    adapter.performPaymentAsync(
                        task.paymentId,
                        task.amount,
                        task.paymentStartedAt,
                        task.deadline
                    )
                } catch (e: Exception) {
                    logger.error("[${properties.accountName}] Payment failed: ${task.paymentId}", e)
                } finally {
                    parallelSemaphore.release()
                }
            }
        } catch (e: RejectedExecutionException) {
            parallelSemaphore.release()
            logger.error("[${properties.accountName}] Executor rejected payment ${task.paymentId}", e)
        }
    }

    fun name() = properties.accountName

    fun availableSlots(): Int = queue.remainingCapacity()
}