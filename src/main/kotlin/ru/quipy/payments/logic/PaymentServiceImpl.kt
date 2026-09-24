package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val workers: List<AccountWorker>
    private val roundRobinCounter = AtomicInteger(0)

    init {
        workers = paymentAccounts.map { adapter ->
            val props = (adapter as PaymentExternalSystemAdapterImpl).properties
            AccountWorker(props, adapter).also { it.start() }
        }
        logger.info("Initialized ${workers.size} account workers: ${workers.map { it.name() }}")
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        val worker = pickWorker()
        val task = AccountWorker.PaymentTask(
            paymentId = paymentId,
            amount = amount,
            paymentStartedAt = paymentStartedAt,
            deadline = deadline,
        )

        if (!worker.trySubmit(task)) {
            logger.warn("Worker ${worker.name()} rejected payment $paymentId (queue full)")
        }
    }

    private fun pickWorker(): AccountWorker {
        val idx = roundRobinCounter.getAndIncrement() % workers.size
        return workers[idx]
    }
}